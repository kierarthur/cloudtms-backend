import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath,pathToFileURL} from 'node:url';
const root=process.argv[2],here=path.dirname(fileURLToPath(import.meta.url));
if(!root)throw Error('ROOT_REQUIRED');
const sha=x=>createHash('sha256').update(x).digest('hex');
const frozen=(ref,file)=>execFileSync('git',['show',`${ref}:${file}`],{cwd:root,maxBuffer:30_000_000});
const json=(ref,file)=>JSON.parse(frozen(ref,file));
const sourceCommit='d863577918ce7d439206296e91ad78de298e98b2',pendingCommit='e8cc61559dd4a16fc9c9dd088fc5ad96f48af342',invoiceCommit='1808fbad03a93abe0ac414938c6e8785e673bae3',auditCommit='fade418e18bce0c121ede3d0298c81377cec8f52';
const auditPath='docs/healthroster-build/proposals/v6-health-completeness-read-audit-20261016/SOURCE_ASSEMBLY_REVIEW.json';
const audit=json(auditCommit,auditPath);
if(sha(frozen(auditCommit,auditPath))!=='030f14063c6c7123dcd6ab7cfc08ac55b3e98b40dcc52b7659161f3689fbeea0')throw Error('AUDIT_DRIFT');
const {scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')));
const {routineDefinitions}=await import(pathToFileURL(path.join(root,'scripts/bpay-next-final-definers.mjs')));
const out=path.join(here,'managed'),written=[],payloads=[],functionPins=[],suppressed=[],effects=[];
function write(file,text){const p=path.join(out,file);fs.mkdirSync(path.dirname(p),{recursive:true});fs.writeFileSync(p,text);written.push({path:file,sha256:sha(text),bytes:Buffer.byteLength(text)});}
const banner=(kind,name)=>`-- ${kind} CloudTMS ${kind==='One-time'?'schema/data migration':'function/view authority'}: ${name}\n-- Source-only Health assembly; actual cross-owner dependencies remain mandatory.\n\n\\set ON_ERROR_STOP on\n\nbegin;\n\n`;
const names=['health_metadata','retained_native_preparations','acceptance_applications_and_adoptions','canonical_publication_bridge','owner_only_rls','incident_resolution'];
const sourceSelections=audit.ordered_selections.filter(x=>x.package==='source');
if(sourceSelections.length!==52)throw Error('SOURCE_SELECTION_NOT52');
const triggerSpecs=[];
function addTrigger(name,schema,table,fn,type,deferred,ddl,origin){triggerSpecs.push({name,schema,table,qualifiedTable:schema+'.'+table,function:fn,type,deferred,createSql:ddl,origin});}
let joined='-- Complete reviewed d863 Health functions, native adapters and retained ACL/effects.\n-- Banking-owned forward18 and forward19 suffix are explicitly excluded.\n';
for(const selection of sourceSelections){
 const bytes=frozen(selection.commit,selection.path);if(sha(bytes)!==selection.whole_file_sha256)throw Error('SOURCE_PIN '+selection.path);
 if(selection.selection==='EXCLUDE_BANKING_OWNED_WHOLE_FILE'){payloads.push({...selection,emitted:false,reason:'Banking-owned body/owner/ACL wholly excluded'});continue;}
 let selected=bytes;if(selection.selection==='EXACT_HEALTH_PREFIX_ONLY_BANK_BODY_EXCLUDED'){selected=bytes.subarray(selection.byte_start,selection.byte_end_exclusive);if(sha(selected)!==selection.selected_sha256||sha(bytes.subarray(selection.byte_end_exclusive))!==selection.excluded_suffix_sha256)throw Error('PREFIX_DRIFT');}
 const source=selected.toString('utf8').replace(/\r\n/g,'\n');let emitted=source;
 const statements=scanManagedSql(source).statements;
 const removals=[];
 for(const s of statements){const text=source.slice(s.start,s.end).trim(),signatures=routineDefinitions(text,selection.path);for(const signature of signatures){if(signature.startsWith('private.bpay_'))throw Error('BANK_BODY_SELECTED');functionPins.push({stage:'BASE',signature,commit:selection.commit,path:selection.path,completeStatementSha256:sha(text)});}
  if(selection.path.includes('/forward/')&&/\bdo\s+\$guards\$/i.test(text)){
   if(!selection.path.endsWith('/01_health_metadata_guards.sqlinc'))throw Error('UNKNOWN_DYNAMIC_TRIGGER');
   const m=text.match(/foreach name in array array\[([\s\S]*?)\] loop/);if(!m)throw Error('METADATA_TABLES_MISSING');
   const tables=[...m[1].matchAll(/'([^']+)'/g)].map(x=>x[1]);if(tables.length!==11)throw Error('METADATA_TABLE_COUNT');
   for(const table of tables)addTrigger('source_pay_metadata_immutable','private',table,'private.weekly_source_pay_decision_metadata_guard_v1()',31,false,`create trigger source_pay_metadata_immutable before insert or update or delete on private.${table} for each row execute function private.weekly_source_pay_decision_metadata_guard_v1();`,{path:selection.path,statementSha256:sha(text),derivation:'Exact11constant FOREACH names and original native FORMAT template'});
   removals.push(s);suppressed.push({path:selection.path,statement:text,statementSha256:sha(text),reason:'Exact native trigger setup relocated to bounded replay-safe Health-only effect successor',triggerCount:11});
  }else if(selection.path.includes('/forward/')&&/^create\s+(?:constraint\s+)?trigger\b/i.test(text)){
   const m=text.match(/^create\s+(constraint\s+)?trigger\s+(\w+)\s+(before|after)\s+([\s\S]*?)\s+on\s+(\w+)\.(\w+)\s+([\s\S]*?)execute function\s+(private\.\w+\(\))\s*;/i);if(!m)throw Error('UNRECOGNIZED_TRIGGER '+text);
   if(!['source_pay_application_must_commit','source_pay_assembly_must_have_one_cut','source_pay_publication_adoption_exact','source_pay_decision_incident_guard'].includes(m[2]))throw Error('UNEXPECTED_TRIGGER '+m[2]);
   const type=1+(m[3].toLowerCase()==='before'?2:0)+(m[4].match(/insert/i)?4:0)+(m[4].match(/delete/i)?8:0)+(m[4].match(/update/i)?16:0);
   addTrigger(m[2],m[5],m[6],m[8],type,Boolean(m[1]),text,{path:selection.path,statementSha256:sha(text),derivation:'Exact complete native CREATE TRIGGER statement'});
   removals.push(s);suppressed.push({path:selection.path,statement:text,statementSha256:sha(text),reason:'Exact native trigger setup relocated to bounded replay-safe Health-only effect successor',triggerCount:1});
  }else if(!signatures.length)effects.push({stage:selection.path.includes('/one-time/')?'SCHEMA':'BASE',path:selection.path,statementSha256:sha(text),statement:text});
 }
 for(const s of removals.sort((a,b)=>b.start-a.start))emitted=emitted.slice(0,s.start)+emitted.slice(s.end);
 if(selection.path.includes('/one-time/')){
  const n=Number(path.basename(selection.path).slice(0,2)),name='nhsp_v6_source_'+names[n-1],file=`supabase/migrations/16102026_11${String(n-1).padStart(2,'0')}_${name}.sql`;
  const guard=n===6?fs.readFileSync(path.join(here,'guards/06_incident_resolution_predecessor.sqlinc'),'utf8')+'\n':'';
  write(file,banner('One-time',name)+`-- Exact ${selection.commit}:${selection.path}\n`+guard+emitted+'\ncommit;\n');
  payloads.push({...selection,emitted:true,selectedCanonicalLfSha256:sha(source),emittedSha256:sha(emitted),managedPath:file,guardAdded:n===6});
 }else{
  const start=Buffer.byteLength(joined);joined+=`\n-- BEGIN exact selected ${selection.commit}:${selection.path}\n`+emitted+`\n-- END exact selected ${selection.path}\n`;
  payloads.push({...selection,emitted:true,selectedCanonicalLfSha256:sha(source),emittedSha256:sha(emitted),emittedByteStart:start,emittedByteEnd:Buffer.byteLength(joined),managedPath:'supabase/repeatable/16102026_1100_nhsp_v6_source_pay_authority/01_complete_source_authorities.sqlinc',suppressedNativeTriggerStatements:removals.length});
 }
}
if(functionPins.length!==76||suppressed.length!==5||triggerSpecs.length!==15)throw Error('BASE_FINITE_COUNTS');
let triggerSql='-- Complete finite setup for the exact15Health-owned d863 triggers.\n-- Existing mismatches fail; no existing trigger is dropped, rewritten or enabled.\n';
for(const [i,t]of triggerSpecs.entries()){
 const tag=`source_trigger_${String(i+1).padStart(2,'0')}`,deferred=t.deferred?'true':'false';
 triggerSql+=`\nlock table ${t.qualifiedTable} in share row exclusive mode;\ndo $${tag}$\nbegin\n if not exists(select 1 from pg_catalog.pg_trigger x where x.tgrelid='${t.qualifiedTable}'::regclass and x.tgname='${t.name}') then\n  ${t.createSql}\n elsif not exists(select 1 from pg_catalog.pg_trigger x\n  where x.tgrelid='${t.qualifiedTable}'::regclass and x.tgname='${t.name}'\n   and x.tgfoid='${t.function}'::regprocedure and x.tgtype=${t.type}\n   and not x.tgisinternal and x.tgenabled='O' and x.tgparentid=0\n   and x.tgattr=''::int2vector and x.tgnargs=0 and x.tgargs=''::bytea\n   and x.tgqual is null and x.tgoldtable is null and x.tgnewtable is null\n   and x.tgconstrrelid=0 and x.tgconstrindid=0\n   and x.tgdeferrable=${deferred} and x.tginitdeferred=${deferred}\n   and ${t.deferred?`x.tgconstraint<>0 and exists(select 1 from pg_catalog.pg_constraint c where c.oid=x.tgconstraint\n    and c.conname='${t.name}' and c.contype='t' and c.conrelid=x.tgrelid\n    and c.condeferrable and c.condeferred and c.convalidated)`:'x.tgconstraint=0'}) then\n  raise exception 'SOURCE_HEALTH_TRIGGER_PREDECESSOR_MISMATCH' using errcode='55000';\n end if;\nend;\n$${tag}$;\n`;
}
fs.mkdirSync(path.join(here,'effects'),{recursive:true});fs.writeFileSync(path.join(here,'effects/01_health_trigger_exact_replay.sqlinc'),triggerSql);
const baseName='16102026_1100_nhsp_v6_source_pay_authority',pendingName='16102026_1110_nhsp_v6_source_pending_current',dutyName='16102026_1120_nhsp_v6_source_invoice_duty';
write(`supabase/repeatable/${baseName}/01_complete_source_authorities.sqlinc`,joined);
write(`supabase/repeatable/${baseName}/02_exact_health_trigger_effects.sqlinc`,triggerSql);
write(`supabase/repeatable/${baseName}_v1.sql`,banner('Repeatable','nhsp_v6_source_pay_authority_v1')+`-- Actual Bank relations/keys and all native owners are required; no fallback schema.\n\\ir ${baseName}/01_complete_source_authorities.sqlinc\n\\ir ${baseName}/02_exact_health_trigger_effects.sqlinc\n\nnotify pgrst,'reload schema';\n\ncommit;\n`);
const pendingSelections=audit.ordered_selections.filter(x=>x.package==='source_pending');
if(pendingSelections.length!==7)throw Error('PENDING_SELECTION_NOT7');
let pending='-- Full e8cc certified pending/current consumers and dedicated immutable Invoice duty scope.\n';
for(const s of pendingSelections){if(s.commit!==pendingCommit)throw Error('PENDING_COMMIT');const b=frozen(s.commit,s.path);if(sha(b)!==s.whole_file_sha256)throw Error('PENDING_SHA');const text=b.toString('utf8').replace(/\r\n/g,'\n');for(const stmt of scanManagedSql(text).statements){const sql=text.slice(stmt.start,stmt.end).trim();for(const signature of routineDefinitions(sql,s.path))functionPins.push({stage:'PENDING_CURRENT',signature,commit:s.commit,path:s.path,completeStatementSha256:sha(sql)});}pending+=`\n-- BEGIN exact selected ${s.commit}:${s.path}\n`+text+`\n-- END exact selected ${s.path}\n`;payloads.push({...s,emitted:true,selectedCanonicalLfSha256:sha(text),managedPath:`supabase/repeatable/${pendingName}/01_complete_pending_current.sqlinc`});}
write(`supabase/repeatable/${pendingName}/01_complete_pending_current.sqlinc`,pending);
write(`supabase/repeatable/${pendingName}_v1.sql`,banner('Repeatable','nhsp_v6_source_pending_current_v1')+`-- Full1100Source foundation and genuine Physical/Candidate/Invoice precede.\n\\ir ${pendingName}/01_complete_pending_current.sqlinc\n\nnotify pgrst,'reload schema';\n\ncommit;\n`);
const dutyPath='docs/healthroster-build/proposals/v6-invoice-duty-scope-successor-20261016/COMPLETE_DUTY_SUCCESSOR.sqlinc',duty=frozen(invoiceCommit,dutyPath).toString('utf8').replace(/\r\n/g,'\n');if(sha(duty)!=='4ef2534740ff7994e263e605196e0a8b9355eb3c4d42c0c190543d2a713dad5c')throw Error('DUTY_SHA');
for(const s of scanManagedSql(duty).statements){const sql=duty.slice(s.start,s.end).trim();for(const signature of routineDefinitions(sql,dutyPath))functionPins.push({stage:'INVOICE_DUTY',signature,commit:invoiceCommit,path:dutyPath,completeStatementSha256:sha(sql)});}
write(`supabase/repeatable/${dutyName}/01_complete_invoice_duty.sqlinc`,duty);
write(`supabase/repeatable/${dutyName}_v1.sql`,banner('Repeatable','nhsp_v6_source_invoice_duty_v1')+`-- Source1110helper precedes this complete one-call Invoice successor.\n\\ir ${dutyName}/01_complete_invoice_duty.sqlinc\n\nnotify pgrst,'reload schema';\n\ncommit;\n`);
payloads.push({package:'invoice_duty',commit:invoiceCommit,path:dutyPath,whole_file_sha256:sha(frozen(invoiceCommit,dutyPath)),selectedCanonicalLfSha256:sha(duty),emitted:true,managedPath:`supabase/repeatable/${dutyName}/01_complete_invoice_duty.sqlinc`});
const finalDefinitions=new Map();for(const f of functionPins)finalDefinitions.set(f.signature,f);
const manifest={version:'HEALTH_SOURCE_COMPLETE_MANAGED_ASSEMBLY_PROPOSAL_V1',qualification:'SOURCE_ONLY_DEPENDENCY_HELD_NOT_INSTALLABLE_OR_NATIVE_ACCEPTED',sourceCommits:{foundation:sourceCommit,pendingCurrent:pendingCommit,invoiceDuty:invoiceCommit,audit:auditCommit},selectionAudit:{commit:auditCommit,path:auditPath,sha256:sha(frozen(auditCommit,auditPath))},counts:{frozenSourceRecords:52,selectedSourcePayloads:51,baseHealthCompleteFunctions:76,pendingCurrentCompleteFunctions:6,invoiceDutyCompleteFunctions:1,completeFunctionDeclarations:functionPins.length,uniqueFinalFunctions:finalDefinitions.size,suppressedNativeSetupStatements:suppressed.length,exactHealthTriggerSet:triggerSpecs.length,managedFiles:written.length},payloads,functions:functionPins,finalDefinitions:[...finalDefinitions.values()],retainedNonFunctionEffects:effects,suppressedNativeTriggerSetupStatements:suppressed,triggerSpecs,guards:{incident:{path:'guards/06_incident_resolution_predecessor.sqlinc',sha256:sha(fs.readFileSync(path.join(here,'guards/06_incident_resolution_predecessor.sqlinc'))),contractCommit:'c1ebb691feb6a11324060e0c28210e2bd735624a',contractPath:'supabase/release/current-contract.json',nativeMigrationCommit:'a934ee1d460b444939234e812babccb90dc3ddff',nativeMigrationPath:'supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql',scope:'Lock exact table, STRICT named local validated native CHECK + text nullable column + exact6predecessorvalues; preserve complete d863 addition after guard'},triggers:{path:'effects/01_health_trigger_exact_replay.sqlinc',sha256:sha(triggerSql),scope:'Create only missing exact15Healthtriggers; existing fullcatalogshape must match; no DROP/replace/enable or Banking trigger'}},bankingExclusions:audit.excluded_banking_body,releaseHold:{active:true,reason:'Actual cross-owner Bank schema/keys/rowtypes/SQL-callees and current financial producers require review/native proof. No fake schema/ABI, disabled checking, Bank body or swallowed financial failure.',creationTimeForeignKeys:audit.creation_time_bank_foreign_keys,creationTimeRowtypes:audit.creation_time_bank_rowtypes,sqlLanguageCreationDependencies:audit.sql_language_creation_callees_and_bank_relations,qualifiedBankRuntimeCallNames:audit.qualified_banking_runtime_call_names_from_selected_plpgsql,externalDependenciesVerbatim:audit.frozen_external_dependencies_verbatim,bankingQualification:audit.banking_qualification_hold,readResult:'SOURCE_PAY_DECISION_READ_PROJECTION_DEPENDENCY_REQUIRED remains exact and truthful until genuine B051 child/public financial-result consumer is complete'},output:written,native:'NOT_RUN',rootEdits:false,bankBodyEdits:false,databaseConnections:0};
if(functionPins.length!==83||finalDefinitions.size!==81||written.length!==13)throw Error('FINAL_COUNTS '+JSON.stringify(manifest.counts));
fs.writeFileSync(path.join(here,'ASSEMBLY_MANIFEST.json'),JSON.stringify(manifest,null,2)+'\n');console.log(JSON.stringify({counts:manifest.counts,manifestSha256:sha(fs.readFileSync(path.join(here,'ASSEMBLY_MANIFEST.json')))},null,2));
