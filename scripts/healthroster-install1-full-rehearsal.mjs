// Source compiler and root-owned native callback. Importing opens no connection.
// Native execution is deliberately impossible through this file's CLI.
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {inventory,mapExecutableSqlSource,logicalPostgresExecutablePolicy,canonicalContractHash} from './cloudtms-db-release-lib.mjs';
import {pendingInventory,compileManagedUnit} from './cloudtms-managed-release.mjs';
import {closeFinalDefinerSelection} from './bpay-next-final-definers.mjs';
import {readerReleasePhases,READER_PREPARATION,readerActivationSql} from './weekly-source-reader-release.mjs';
import {canonical,digest,scanManagedSql,exactAtomicBody,literal} from './cloudtms-managed-release-sql.mjs';
import {neutraliseReviewedVerifierEnvelope,renderFullRehearsalUnits,closedRehearsalDiagnostic} from './healthroster-full-rehearsal-protocol.mjs';
import {assertTurnIdentity,ownTurnActivityExclusionSql} from './healthroster-turn-control.mjs';
import {verifierClosure} from './healthroster-install1-verifier-census.mjs';

export const REHEARSAL_VERSION='HEALTHROSTER_INSTALL1_FULL_REHEARSAL_V1';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const finalBrowser='supabase/verification/22082026_1302_general_browser_isolation_verification.sql';
const sourcePinPattern=/^[0-9a-f]{40}$/;
const hashPattern=/^[0-9a-f]{64}$/;
const markerPattern=/^hr_install1_[a-f0-9]{32}$/;
const requireThat=(condition,code)=>{if(!condition)throw Error(code);};
const stable=value=>Array.isArray(value)?value.map(stable):value&&typeof value==='object'
 ?Object.fromEntries(Object.keys(value).sort().map(key=>[key,stable(value[key])])):value;
const releaseInputFiles=['scripts/cloudtms-db-release-lib.mjs','scripts/cloudtms-managed-release.mjs',
 'scripts/cloudtms-managed-release-sql.mjs','scripts/cloudtms-managed-execution-policy.mjs','scripts/bpay-next-final-definers.mjs',
 'scripts/weekly-source-reader-release.mjs','supabase/release/current-release.json','supabase/release/migration-lock.json',
 'supabase/release/protected-boundary-lock.json','supabase/release/export_contract.sql','docs/bpay-next/final-definer-closures.json'];

// Root runs this exact source literal as a separate fresh read-only probe under
// the already-held turn. PostgreSQL itself supplies jsonb formatting; no client
// formatter, server catalogue adoption, or huge full-rehearsal literal is used.
export function expectedContractDigestSql({expectedContractHash}={}){
 const contract=JSON.parse(fs.readFileSync(path.join(root,'supabase/release/current-contract.json'),'utf8'));
 requireThat(hashPattern.test(expectedContractHash??'')&&canonicalContractHash(contract)===expectedContractHash,
  'REHEARSAL_EXPECTED_CONTRACT_DIGEST_SOURCE_DRIFT');
 const text=JSON.stringify(contract),tag='$hr_expected_contract_'+expectedContractHash.slice(0,32)+'$';
 requireThat(!text.includes(tag),'REHEARSAL_EXPECTED_CONTRACT_LITERAL_COLLISION');
 return `select jsonb_build_object('expectedContractHash',${literal(expectedContractHash)},
  'expectedJsonbHash',encode(pg_catalog.sha256(pg_catalog.convert_to(${tag}${text}${tag}::jsonb::text,'UTF8')),'hex'),
  'physicalDatabase',current_database(),'identityTest',exists(select 1 from private.cloudtms_database_identity where singleton and environment='TEST' and customer_key is null),
  'customerKeyNull',exists(select 1 from private.cloudtms_database_identity where singleton and customer_key is null));`;
}

function markerContractRelation(marker){
 requireThat(markerPattern.test(marker??''),'REHEARSAL_MARKER_REFUSED');
 return {acl:['DELETE','INSERT','MAINTAIN','REFERENCES','SELECT','TRIGGER','TRUNCATE','UPDATE']
  .map(privilege=>({grantee:'postgres',grantable:false,privilege})),rls:false,kind:'r',name:marker,owner:'postgres',schema:'private',
  columns:[{name:'marker',type:'boolean',default:null,identity:'',not_null:true,generated:''}],indexes:[],options:[],force_rls:false,
  constraints:[],view_definition_sha256:null};
}

export function rehearsalSourceBinding({current=inventory(),review,contractPath='supabase/release/current-contract.json'}={}){
 requireThat(contractPath==='supabase/release/current-contract.json','REHEARSAL_CONTRACT_PATH_REFUSED');
 const contract=JSON.parse(fs.readFileSync(path.join(root,contractPath),'utf8'));
 const inputs=releaseInputFiles.map(file=>({file,sha256:digest(canonical(fs.readFileSync(path.join(root,file),'utf8')))}));
 const value={contractSha256:canonicalContractHash(contract),inputs,inventorySha256:digest(JSON.stringify(current)),
  reviewSha256:digest(JSON.stringify(review))};
 return {...value,sha256:digest(JSON.stringify(value))};
}

export function assertRehearsalSourceBinding(binding,{review,current=inventory(),expectedContractHash}={}){
 const fresh=rehearsalSourceBinding({current,review});
 requireThat(hashPattern.test(expectedContractHash??'')&&fresh.contractSha256===expectedContractHash
  &&JSON.stringify(stable(binding))===JSON.stringify(stable(fresh)),'REHEARSAL_RELEASE_SOURCE_BINDING_DRIFT');
 return fresh;
}

function exporterQuery(exportContractSql){
 const expected=canonical(fs.readFileSync(path.join(root,'supabase/release/export_contract.sql'),'utf8'));
 requireThat(canonical(exportContractSql)===expected,'REHEARSAL_CONTRACT_EXPORT_SOURCE_DRIFT');
 const parsed=scanManagedSql(expected);
 requireThat(parsed.statements.length===2&&parsed.statements[0].tokens.join(' ')==='SET SEARCH_PATH = PG_CATALOG , PUBLIC'
  &&parsed.statements[1].tokens[0]==='WITH','REHEARSAL_CONTRACT_EXPORT_QUERY_REFUSED');
 return expected.slice(parsed.statements[1].start,parsed.statements[1].end).trim().replace(/;\s*$/,'');
}

// Only literal, closed source codes can leave the database diagnostics boundary.
// A placeholder, identifier, DETAIL, CONTEXT or arbitrary primary text cannot.
export function reviewedDiagnosticCodes(review){
 const codes=new Set();
 for(const row of review.sourceFiles??[]){
  requireThat(/^(?:supabase|tests)\/[A-Za-z0-9_./-]+$/.test(row.file)&&!row.file.split('/').includes('..')
   &&hashPattern.test(row.sha256),'REHEARSAL_DIAGNOSTIC_SOURCE_PATH_REFUSED');
  const source=canonical(fs.readFileSync(path.join(root,row.file),'utf8'));
  requireThat(digest(source)===row.sha256,'REHEARSAL_DIAGNOSTIC_SOURCE_DRIFT');
  for(const match of source.matchAll(/(?:raise\s+exception(?:\s+using\s+message\s*=)?|\bmessage\s*=)\s*'([A-Z][A-Z0-9_]{5,})(?=\s|:|'|%)/gi)){
   if(/^[A-Z][A-Z0-9_]{5,}$/.test(match[1]))codes.add(match[1]);
  }
 }
 return [...codes].sort();
}

export function assertInstalledRehearsalContract(actual,{expectedContractHash,marker}){
 requireThat(hashPattern.test(expectedContractHash??'')&&markerPattern.test(marker??'')&&Array.isArray(actual?.relations),
  'REHEARSAL_INSTALLED_CONTRACT_INPUT_REFUSED');
 const found=actual.relations.filter(row=>row.schema==='private'&&row.name===marker);
 const expected=markerContractRelation(marker);
 requireThat(found.length===1&&JSON.stringify(stable(found[0]))===JSON.stringify(stable(expected)),
  'REHEARSAL_MARKER_CONTRACT_SHAPE_REFUSED');
 requireThat(['triggers','policies'].every(section=>Array.isArray(actual[section])
  &&!actual[section].some(row=>row.schema==='private'&&row.table===marker)),'REHEARSAL_MARKER_AUTHORITY_REFUSED');
 // Remove this one exact relation only. Preserve every other exporter byte,
 // including all security sections, sibling names and object array ordering.
 const clean={...actual,relations:actual.relations.filter(row=>row!==found[0])};
 const sha256=canonicalContractHash(clean);
 requireThat(sha256===expectedContractHash,'REHEARSAL_INSTALLED_CONTRACT_MISMATCH');
 return {matched:true,sha256,markerRelationChecked:true,omittedRelation:'private.'+marker,
  sectionCounts:Object.fromEntries(Object.entries(clean).filter(([,value])=>Array.isArray(value)).map(([key,value])=>[key,value.length]))};
}
const statementObject=row=>{
 const tokens=row.tokens;
 for(let i=0;i<tokens.length-2;i++)if(['PUBLIC','PRIVATE','PG_TEMP'].includes(tokens[i])&&tokens[i+1]==='.'&&/^[A-Z_][A-Z0-9_$]*$/.test(tokens[i+2]))return tokens.slice(i,i+3).join('').toLowerCase();
 return tokens[0]?.toLowerCase()??null;
};
const clientVariables=sql=>[...new Set(scanManagedSql(sql).meta.flatMap(row=>{
 const set=row.text.match(/^\\(?:set|unset)\s+([A-Za-z_][A-Za-z0-9_]*)\b/i);
 const refs=[...row.text.matchAll(/:\{\?([A-Za-z_][A-Za-z0-9_]*)\}|:(?:'|")?([A-Za-z_][A-Za-z0-9_]*)/g)].map(x=>x[1]??x[2]);
 return [...set?[set[1]]:[],...refs];
}))].filter(name=>!['ON_ERROR_STOP','ON_ERROR_ROLLBACK'].includes(name)).sort();

// Map each physical file with the candidate's bounded audited owner mapper,
// before expanding its reviewed includes. Never alter canonical source hashes.
export function mappedVerifier(file,stack=[]){
 const original=verifierClosure(file,root),source=mapExecutableSqlSource(original.source,file),parsed=scanManagedSql(source),replacements=[];
 for(const row of parsed.meta.filter(x=>/^\\ir\s/i.test(x.text))){
  const match=row.text.match(/^\\ir\s+(?:'([^']+)'|"([^"]+)"|([^\s;]+))\s*;?\s*$/i);
  requireThat(match,'REHEARSAL_MAPPED_INCLUDE_REFUSED');
  const child=path.posix.normalize(path.posix.join(path.posix.dirname(file),match[1]??match[2]??match[3]));
  requireThat(!stack.includes(child),'REHEARSAL_MAPPED_INCLUDE_CYCLE');
  replacements.push({...row,value:mappedVerifier(child,[...stack,file]).sql});
 }
 let sql=source;for(const row of replacements.reverse())sql=sql.slice(0,row.start)+row.value+sql.slice(row.end);
 return {sql,original};
}

export function assertReview(review,release){
 requireThat(review?.version===REHEARSAL_VERSION&&Array.isArray(review.verifiers),'REHEARSAL_REVIEW_REQUIRED');
 requireThat(review.verifiers.length===release.verificationFiles.length,'REHEARSAL_REVIEW_CENSUS_REFUSED');
 for(const [index,file] of release.verificationFiles.entries()){
  const row=review.verifiers[index];
  requireThat(row?.file===file&&hashPattern.test(row.sourceSqlHash??'')&&hashPattern.test(row.effectGraphHash??'')
   &&['INCLUDE','EXCLUDE'].includes(row.decision)&&typeof row.reason==='string'&&row.reason.length>10,'REHEARSAL_REVIEW_ROW_REFUSED');
 }
 return review;
}

function readerPreparationUnit(readers){
 const sql=`do $hr_reader_preparation$ declare ready boolean; begin
 for page_number in 1..${READER_PREPARATION.maximumCallsPerInvocation} loop
  ready := ${READER_PREPARATION.pageRoutine}();
  if ready is true then return; end if;
  if ready is distinct from false then raise exception 'READER_PREPARATION_INVALID_RESULT'; end if;
 end loop;
 raise exception 'READER_PREPARATION_RESUME_REQUIRED';
 end $hr_reader_preparation$;`;
 return {file:readers[0].path,kind:'repeatable',sql,sqlHash:digest(sql),phase:'source_reader_preparation',
  sourceAuthority:READER_PREPARATION,sourceSqlHash:digest(JSON.stringify(READER_PREPARATION))};
}

// This compiler takes the freshly observed installed ledgers. It performs no
// probe, credential read, promotion, release dispatch, or native execution.
export function compileFullRehearsal({candidatePin,currentRemoteTestPin,installed,review,marker,
 expectedContractHash,contractDigest,
 current=inventory(),release=JSON.parse(fs.readFileSync(path.join(root,'supabase/release/current-release.json'),'utf8')),
 closeSelection=closeFinalDefinerSelection,compileUnit=compileManagedUnit,readVerifier=mappedVerifier,
 readerPhases=readerReleasePhases,render=renderFullRehearsalUnits}={}){
 requireThat(sourcePinPattern.test(candidatePin??'')&&sourcePinPattern.test(currentRemoteTestPin??''),'REHEARSAL_COMMIT_INPUT_REFUSED');
 requireThat(/^hr_install1_[a-f0-9]{32}$/.test(marker??''),'REHEARSAL_MARKER_REFUSED');
 requireThat(process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER==='CURRENT_USER','REHEARSAL_AUDITED_OWNER_MODE_REQUIRED');
 assertReview(review,release);
 requireThat(review.sourceCounts?.migrations===current.migrations.length&&review.sourceCounts?.repeatables===current.repeatables.length
  &&review.sourceCounts?.verifiers===release.verificationFiles.length,'REHEARSAL_REVIEW_INVENTORY_DRIFT');
 requireThat(Array.isArray(review.sourceFiles)&&review.sourceFiles.length>0,'REHEARSAL_CALLEE_SOURCE_CENSUS_REQUIRED');
 for(const row of review.sourceFiles){
  requireThat(/^(?:supabase|tests)\/[A-Za-z0-9_./-]+$/.test(row.file)&&!row.file.split('/').includes('..')
   &&hashPattern.test(row.sha256),'REHEARSAL_CALLEE_SOURCE_PATH_REFUSED');
  requireThat(digest(canonical(fs.readFileSync(path.join(root,row.file),'utf8')))===row.sha256,'REHEARSAL_CALLEE_SOURCE_DRIFT');
 }
 const pending=closeSelection(pendingInventory(current,installed),current);
 const phases=readerPhases(pending.repeatables,current.repeatables),units=[];
 const options={root,mapSource:mapExecutableSqlSource};
 for(const [kind,rows] of [['migration',pending.migrations],['repeatable',phases.ordinary]])for(const item of rows){
  const compiled=compileUnit(item,kind,options);
  // A protected release's exception may contain transaction commits or gexec.
  // It cannot be silently flattened into this rollback-only outer transaction.
  requireThat(!compiled.exception&&!compiled.ownerRlsEnvelope&&!compiled.immutableGuardDependency,'REHEARSAL_SPECIAL_MANAGED_ENVELOPE_REVIEW_REQUIRED');
  units.push({file:item.path,kind,sql:compiled.body,sqlHash:digest(compiled.body),sourceSqlHash:item.sha256,phase:kind,
   sourceFiles:compiled.closure?.ordered.map(x=>({file:x.path,sha256:digest(x.source)}))??[]});
 }
 if(phases.readers.length){
  for(const item of phases.readers)compileUnit(item,'repeatable',options);
  units.push(readerPreparationUnit(phases.readers));
  const activation=readerActivationSql(phases.readers,file=>mapExecutableSqlSource(canonical(fs.readFileSync(path.join(root,file),'utf8')),file));
  const sql=exactAtomicBody(activation);
  units.push({file:phases.readers[0].path,kind:'repeatable',sql,sqlHash:digest(sql),phase:'source_reader_activation',
   sourceFiles:phases.readers.map(x=>({file:x.path,sha256:x.sha256})),sourceSqlHash:digest(activation)});
 }
 const exclusions=[];
 for(const [index,file] of release.verificationFiles.entries()){
  const reviewed=review.verifiers[index],{sql,original}=readVerifier(file);
  requireThat(original.sourceSqlHash===reviewed.sourceSqlHash&&original.closureHash===reviewed.closureHash,'REHEARSAL_VERIFIER_SOURCE_DRIFT');
  requireThat(digest(sql)===reviewed.executableSqlHash,'REHEARSAL_VERIFIER_EXECUTABLE_DRIFT');
  if(reviewed.decision==='EXCLUDE'){exclusions.push(reviewed);continue;}
  requireThat(reviewed.pendingIndependentReview!==true,'REHEARSAL_SAFETY_REVIEW_INCOMPLETE');
  const unit={file,kind:'verifier',sql,sqlHash:digest(sql),effectGraphHash:reviewed.effectGraphHash};
  const neutral=neutraliseReviewedVerifierEnvelope(unit,{sourceSqlHash:unit.sqlHash,removedCommands:reviewed.removedCommands,
   readOnlyBodyReviewed:reviewed.readOnlyBodyReviewed===true});
  units.push({...neutral,phase:'mandatory_upgrade_verifier',clientVariables:clientVariables(neutral.sql)});
 }
 const finalReview=review.verifiers.find(x=>x.file===finalBrowser);
 requireThat(finalReview?.decision==='INCLUDE','REHEARSAL_FINAL_BROWSER_REVIEW_REQUIRED');
 const final=mappedVerifier(finalBrowser),unit={file:finalBrowser,kind:'final_browser_verifier',sql:final.sql,sqlHash:digest(final.sql)};
 units.push({...neutraliseReviewedVerifierEnvelope(unit,{sourceSqlHash:unit.sqlHash,removedCommands:finalReview.removedCommands,
  readOnlyBodyReviewed:finalReview.readOnlyBodyReviewed===true}),phase:'final_browser_isolation',clientVariables:clientVariables(final.sql)});
 // Every native REPEATABLE READ assertion is retained. Conflicting native modes
 // are refused by the protocol; SERIALIZABLE is not an implied substitute.
 const rendered=render(units,{outerIsolation:'REPEATABLE READ'});
 const sourceBinding=rehearsalSourceBinding({current,review,contractPath:release.contractPath});
 expectedContractHash??=sourceBinding.contractSha256;
 requireThat(expectedContractHash===sourceBinding.contractSha256,'REHEARSAL_SEALED_CONTRACT_DRIFT');
 const diagnosticCodes=[...new Set([...reviewedDiagnosticCodes(review),'REHEARSAL_MARKER_CONTRACT_SHAPE_REFUSED',
  'REHEARSAL_MARKER_AUTHORITY_REFUSED','REHEARSAL_INSTALLED_CONTRACT_MISMATCH'])].sort(),diagnosticCodesHash=digest(JSON.stringify(diagnosticCodes));
 const query=exporterQuery(fs.readFileSync(path.join(root,'supabase/release/export_contract.sql'),'utf8'));
 requireThat(contractDigest?.connectedFresh===true&&contractDigest.physicalDatabase==='cloudtms_test_clone'
  &&contractDigest.identityTest===true&&contractDigest.customerKeyNull===true&&contractDigest.expectedContractHash===expectedContractHash
  &&hashPattern.test(contractDigest.expectedJsonbHash??'')&&Number.isFinite(Date.parse(contractDigest.checkedAt??''))
  &&Date.now()-Date.parse(contractDigest.checkedAt)>=0&&Date.now()-Date.parse(contractDigest.checkedAt)<15*60*1000,
  'REHEARSAL_EXPECTED_CONTRACT_DIGEST_FRESH_PROBE_REQUIRED');
 const expectedJsonbHash=contractDigest.expectedJsonbHash;
 const contractSql=`set local search_path=pg_catalog,public;
 create temporary table hr_rehearsal_contract_capture on commit drop as ${query};
 do $hr_contract_compare$ declare v_contract jsonb; v_markers jsonb; v_relations jsonb; v_clean jsonb; begin
  select jsonb_build_object into strict v_contract from pg_temp.hr_rehearsal_contract_capture;
  select coalesce(jsonb_agg(entry),'[]'::jsonb) into v_markers from jsonb_array_elements(v_contract->'relations') entry
   where entry->>'schema'='private' and entry->>'name'=${literal(marker)};
  if v_markers is distinct from jsonb_build_array(${literal(JSON.stringify(markerContractRelation(marker)))}::jsonb)
   then raise exception 'REHEARSAL_MARKER_CONTRACT_SHAPE_REFUSED'; end if;
  if exists(select 1 from jsonb_array_elements(v_contract->'triggers') entry where entry->>'schema'='private' and entry->>'table'=${literal(marker)})
   or exists(select 1 from jsonb_array_elements(v_contract->'policies') entry where entry->>'schema'='private' and entry->>'table'=${literal(marker)})
   then raise exception 'REHEARSAL_MARKER_AUTHORITY_REFUSED'; end if;
  select coalesce(jsonb_agg(entry order by ordinal),'[]'::jsonb) into v_relations
   from jsonb_array_elements(v_contract->'relations') with ordinality entries(entry,ordinal)
   where not (entry->>'schema'='private' and entry->>'name'=${literal(marker)});
  v_clean:=jsonb_set(v_contract,'{relations}',v_relations);
  if encode(pg_catalog.sha256(pg_catalog.convert_to(v_clean::text,'UTF8')),'hex')<>${literal(expectedJsonbHash)}
   then raise exception 'REHEARSAL_INSTALLED_CONTRACT_MISMATCH'; end if;
 end $hr_contract_compare$;
 select 'HR_CONTRACT_JSON '||jsonb_build_object::text from pg_temp.hr_rehearsal_contract_capture;`;
 const body=rendered.sql;
 const sql=['\\set ON_ERROR_STOP off','\\set ON_ERROR_ROLLBACK on','\\set VERBOSITY verbose','\\set SHOW_CONTEXT never','\\pset pager off',
  'begin isolation level repeatable read;','set local jit=off;','set local pg_show_plans.is_enabled=off;','set local statement_timeout=0;',
  `create table private.${marker}(marker boolean not null);`,
  `revoke all on table private.${marker} from PUBLIC,anon,authenticated,service_role;`,
  `insert into private.${marker} values(true);`,body,'\\echo HR_CONTRACT_BEGIN',contractSql,'\\echo HR_CONTRACT_END','rollback;',
  '\\echo HR_EXPLICIT_ROLLBACK_COMPLETED'].join('\n')+'\n';
 const bodyStart=sql.indexOf(body),lineOffset=sql.slice(0,bodyStart).split('\n').length-1,lineMap=[];
 const starts=[...body.matchAll(/^\\echo HR_FILE_BEGIN (\d+)$/gm)];
 for(const [unitIndex,entry] of units.entries()){
  const begin=starts[unitIndex]?.index;requireThat(begin!==undefined,'REHEARSAL_STATEMENT_MAP_REFUSED');
  const sqlStart=body.indexOf(entry.sql,begin);requireThat(sqlStart>=0,'REHEARSAL_STATEMENT_MAP_REFUSED');
  for(const [statement,row] of scanManagedSql(entry.sql).statements.entries()){
   const startLine=lineOffset+body.slice(0,sqlStart+row.start).split('\n').length;
   const endLine=lineOffset+body.slice(0,sqlStart+row.end).split('\n').length-1;
   lineMap.push({file:entry.file,kind:entry.kind,phase:entry.phase,statement:statement+1,object:statementObject(row),
    startLine,endLine:Math.max(startLine,endLine),statementHash:digest(entry.sql.slice(row.start,row.end))});
  }
 }
 const contractStart=sql.indexOf(contractSql);
 for(const [statement,row] of scanManagedSql(contractSql).statements.entries())lineMap.push({file:'supabase/release/export_contract.sql',
  kind:'contract_export',phase:'installed_contract_comparison',statement:statement+1,object:'catalogue',
  startLine:sql.slice(0,contractStart+row.start).split('\n').length,endLine:sql.slice(0,contractStart+row.end).split('\n').length,
  statementHash:digest(contractSql.slice(row.start,row.end))});
 return {version:REHEARSAL_VERSION,status:'COMPILED_NOT_RUN',candidatePin,currentRemoteTestPin,marker,mode:'UPGRADE',
  currentCounts:{migrations:current.migrations.length,repeatables:current.repeatables.length,verifiers:release.verificationFiles.length},
  pendingCounts:{migrations:pending.migrations.length,repeatables:pending.repeatables.length,readerPair:phases.readers.length},
  units:rendered.units.map((x,i)=>({...x,phase:units[i].phase,sourceFiles:units[i].sourceFiles??[],clientVariables:units[i].clientVariables??[]})),
  exclusions,exclusionCount:exclusions.length,sql,sqlHash:digest(sql),bytes:Buffer.byteLength(sql),lineMap,
  outerIsolation:rendered.outerIsolation,isolationRequirements:rendered.isolationRequirements,
  sourceBinding,expectedContractHash,expectedJsonbHash,diagnosticCodes,diagnosticCodesHash,
  ownerPolicy:logicalPostgresExecutablePolicy(),nativeFull:'NOT_RUN',mutationsAuthorised:false};
}

export function assertFreshNativeGate(gate,{candidatePin,currentRemoteTestPin,expectedContractHash,now=Date.now()}={}){
 const checked=Date.parse(gate?.checkedAt??'');
 requireThat(Number.isFinite(checked)&&now-checked>=0&&now-checked<15*60*1000,'REHEARSAL_GATE_STALE');
 requireThat(gate.environment==='TEST'&&gate.physicalDatabase==='cloudtms_test_clone'&&gate.identityTest===true&&gate.customerKeyNull===true
  &&gate.module==='LEGACY:1', 'REHEARSAL_TEST_GATE_REFUSED');
 requireThat(gate.candidatePin===candidatePin&&gate.pushedCandidatePin===candidatePin&&gate.cleanGit===true&&gate.sourceStaged===true
  &&gate.remoteTestPin===currentRemoteTestPin&&gate.reviewedSqlRuntimeMatch===true,'REHEARSAL_SOURCE_GATE_REFUSED');
 requireThat(hashPattern.test(expectedContractHash??'')&&gate.expectedContractHash===expectedContractHash,'REHEARSAL_SEALED_CONTRACT_GATE_REFUSED');
 requireThat(hashPattern.test(gate.sourceBindingSha256??''),'REHEARSAL_SOURCE_BINDING_GATE_REFUSED');
 requireThat(gate.bank?.status==='COMPLETE'&&gate.bank?.protectedConclusion==='success'&&gate.bank?.sourcePin===currentRemoteTestPin
  &&gate.bank?.sharedUnlockConfirmed===true&&typeof gate.bank.recordHash==='string'&&hashPattern.test(gate.bank.recordHash),'REHEARSAL_BANK_GATE_REFUSED');
 requireThat(gate.latestRelease?.status==='VERIFIED'&&gate.latestRelease.commit===currentRemoteTestPin,'REHEARSAL_LATEST_RELEASE_GATE_REFUSED');
 requireThat(gate.protectedRunsChecked===true&&gate.protectedActiveCount===0&&gate.applyingCount===0&&gate.phoneOrAcceptanceActive===false,'REHEARSAL_ACTIVITY_GATE_REFUSED');
 requireThat(gate.route==='APPROVED_MACHINE_LOCAL_FALLBACK_PROCESS_ENV'&&gate.borrowedLabCapability===false&&gate.secretsInArgvOrFiles===false,'REHEARSAL_ROUTE_GATE_REFUSED');
 requireThat(gate.ownerNativeCallbackAuthorised===true,'REHEARSAL_NATIVE_CALLBACK_NOT_AUTHORISED');
 return gate;
}

export function assertFreshRestoration(before,after,marker){
 requireThat(after?.connectedFresh===true&&after.physicalDatabase==='cloudtms_test_clone'&&after.identityTest===true
  &&after.customerKeyNull===true&&after.module==='LEGACY:1', 'REHEARSAL_FRESH_IDENTITY_REFUSED');
 requireThat(after.markerAbsent===true&&after.marker===marker,'REHEARSAL_MARKER_PERSISTED');
 for(const key of ['ledgerFingerprint','catalogueFingerprint','businessCountsFingerprint','businessRowsFingerprint']){
  requireThat(hashPattern.test(before?.[key]??'')&&after[key]===before[key],'REHEARSAL_RESTORATION_MISMATCH');
 }
 requireThat(hashPattern.test(before?.relationCensus??'')&&after.relationCensus===before.relationCensus,'REHEARSAL_RELATION_CENSUS_MISMATCH');
 requireThat(after.applyingCount===0&&after.otherActivityCount===0,'REHEARSAL_FRESH_ACTIVITY_REFUSED');
 return {connectedFresh:true,physicalDatabase:after.physicalDatabase,module:after.module,markerAbsent:true,
  compared:['ledgerFingerprint','catalogueFingerprint','businessCountsFingerprint','businessRowsFingerprint']};
}

// Root calls this from ONE withSharedRehearsalTurn work callback. This function
// never acquires/releases a turn, spawns a probe, reads credentials, promotes,
// or dispatches a release independently. Root supplies same-turn implementations.
export async function runFullRehearsalWithinOwnedTurn({turn,candidatePin,currentRemoteTestPin,expectedContractHash,marker,review,callbacks,compilePlan=compileFullRehearsal}){
 assertTurnIdentity(turn?.identity);
 requireThat(turn.signal&&!turn.signal.aborted&&typeof turn.assertHeld==='function','REHEARSAL_OWNED_TURN_REQUIRED');
 for(const name of ['freshGate','before','executeSameSession','freshAfter','recordReceipt','publishManaged'])requireThat(typeof callbacks?.[name]==='function','REHEARSAL_ROOT_CALLBACK_REQUIRED');
 let bindingHash;
 const gate=async phase=>{await turn.assertHeld(phase);if(turn.signal.aborted)throw turn.signal.reason;
  const value=assertFreshNativeGate(await callbacks.freshGate(phase),{candidatePin,currentRemoteTestPin,expectedContractHash});
  if(bindingHash)requireThat(value.sourceBindingSha256===bindingHash,'REHEARSAL_RELEASE_SOURCE_BINDING_DRIFT');
  else bindingHash=value.sourceBindingSha256;return value;};
 await gate('install1-before');
 const before=await callbacks.before({marker,activityExclusionSql:ownTurnActivityExclusionSql(turn.identity),turn});
 requireThat(before.physicalDatabase==='cloudtms_test_clone'&&before.identityTest===true&&before.customerKeyNull===true&&before.module==='LEGACY:1'
  &&before.applyingCount===0&&before.otherActivityCount===0&&before.markerAbsent===true,'REHEARSAL_BEFORE_REFUSED');
 const plan=compilePlan({candidatePin,currentRemoteTestPin,expectedContractHash,contractDigest:before.contractDigest,installed:before.installed,review,marker});
 requireThat(plan.sourceBinding?.sha256===bindingHash,'REHEARSAL_COMPILED_SOURCE_BINDING_DRIFT');
 await gate('install1-full');
 let execution,executionFailure,restoration;
 try{
  execution=await callbacks.executeSameSession({sql:plan.sql,lineMap:plan.lineMap,sqlHash:plan.sqlHash,turn,marker,
   expectedContractHash:plan.expectedContractHash,diagnosticCodes:plan.diagnosticCodes,diagnosticCodesHash:plan.diagnosticCodesHash,
   settings:{jit:'off',collector:'off',statementTimeout:0,onErrorRollback:'on',onErrorStop:'off',outerIsolation:'REPEATABLE READ'},
   noHarnessDeadline:true,diagnostic:line=>closedRehearsalDiagnostic(line,plan.lineMap,new Set(plan.diagnosticCodes))});
  requireThat(execution?.sameSession===true&&execution.explicitRollback===true&&execution.exitCode===0
   &&execution.errorCollectionComplete===true&&Array.isArray(execution.errors),'REHEARSAL_EXECUTION_INCOMPLETE');
  requireThat(execution.installedContract?.matched===true&&execution.installedContract.sha256===plan.expectedContractHash,
   'REHEARSAL_INSTALLED_CONTRACT_NOT_PROVED');
 }catch(error){executionFailure=error;}
 await callbacks.recordReceipt({version:REHEARSAL_VERSION,status:'SQL_PASS_FINISHED_RESTORATION_UNVERIFIED',candidatePin,currentRemoteTestPin,
  sqlHash:plan.sqlHash,execution:execution??null,executionFailure:executionFailure
   ?(/^REHEARSAL_[A-Z_]+$/.test(executionFailure.message)?executionFailure.message:'EXECUTION_ADAPTER_FAILED'):null,
  restorationProved:false,turnId:turn.identity.turnId});
 // Even a failed SQL pass requires fresh rollback evidence, under the held turn.
 await turn.assertHeld('install1-fresh');
 const after=await callbacks.freshAfter({marker,activityExclusionSql:ownTurnActivityExclusionSql(turn.identity),turn});
 try{restoration=assertFreshRestoration(before,after,marker);}catch(error){
  await callbacks.recordReceipt({version:REHEARSAL_VERSION,status:'RESTORATION_FAILED_HOLD_INSTALL',candidatePin,currentRemoteTestPin,
   sqlHash:plan.sqlHash,execution:execution??null,restorationProved:false,turnId:turn.identity.turnId});throw error;
 }
 await callbacks.recordReceipt({version:REHEARSAL_VERSION,status:executionFailure?'EXECUTION_INCOMPLETE_HOLD_INSTALL':execution.errors.length
  ?'FULL_ERRORS_ROLLBACK_PROVED_HOLD_INSTALL':'FULL_CLEAN_ROLLBACK_PROVED',candidatePin,currentRemoteTestPin,sqlHash:plan.sqlHash,
  execution:execution??null,restoration,turnId:turn.identity.turnId});
 if(executionFailure)throw executionFailure;
 requireThat(execution.errors.length===0,'REHEARSAL_FULL_ERRORS_HOLD_INSTALL');
 const receipt={version:REHEARSAL_VERSION,status:'FULL_CLEAN_ROLLBACK_PROVED',candidatePin,currentRemoteTestPin,sqlHash:plan.sqlHash,
  beforeFingerprints:Object.fromEntries(['ledgerFingerprint','catalogueFingerprint','businessCountsFingerprint','businessRowsFingerprint'].map(k=>[k,before[k]])),
  restoration,counts:plan.currentCounts,pending:plan.pendingCounts,exclusions:plan.exclusions,errors:[],turnId:turn.identity.turnId};
 receipt.sourceBinding=plan.sourceBinding;receipt.installedContract=execution.installedContract;
 await gate('install1-before-managed-release');
 // The root callback must use turn.runManagedRelease / canonical npm release:test,
 // and retain this turn through database wait and ordered runtime publication.
 const publication=await callbacks.publishManaged({turn,receipt,candidatePin,currentRemoteTestPin});
 await turn.assertHeld('install1-after-managed-release');
 return {receipt,publication};
}

export const activityGuardSql=identity=>`select count(*) from pg_catalog.pg_stat_activity where datname=current_database() and ${ownTurnActivityExclusionSql(identity)} and (state <> 'idle' or xact_start is not null);`;

// Optional root adapter. Credentials are already loaded by the approved root
// process and travel only in libpq environment variables. Never retain raw
// stdout/stderr in the returned receipt, console, or a file.
export async function executePsqlWithinOwnedTurn({turn,psqlPath,env,sql,lineMap,sqlHash,marker,expectedContractHash,diagnosticCodes=[],diagnosticCodesHash}){
 assertTurnIdentity(turn?.identity);
 requireThat(typeof turn.runOwnedChild==='function'&&path.isAbsolute(psqlPath??'')&&/^psql(?:\.exe)?$/i.test(path.basename(psqlPath)),'REHEARSAL_PSQL_ADAPTER_REFUSED');
 requireThat(env?.PGDATABASE==='cloudtms_test_clone'&&typeof env.PGHOST==='string'&&env.PGHOST.length>0
  &&typeof env.PGUSER==='string'&&typeof env.PGPASSWORD==='string','REHEARSAL_PROCESS_ENV_ROUTE_REQUIRED');
 requireThat(digest(sql)===sqlHash&&Array.isArray(lineMap),'REHEARSAL_PSQL_SOURCE_REFUSED');
 requireThat(digest(JSON.stringify(diagnosticCodes))===diagnosticCodesHash&&diagnosticCodes.every(code=>/^[A-Z][A-Z0-9_]{5,}$/.test(code)),
  'REHEARSAL_DIAGNOSTIC_CODES_REFUSED');
 const result=await turn.runOwnedChild({file:psqlPath,args:['-X','-q','-A','-t'],input:sql,
  options:{env:{...env,PGAPPNAME:'healthroster_install1_full',PGOPTIONS:(env.PGOPTIONS??'')+' -c jit=off -c pg_show_plans.is_enabled=off -c statement_timeout=0'},
   stdio:['pipe','pipe','pipe']},cancelOnLoss:true});
 const errors=[];
 let unexpectedError=false;
 for(const line of result.stderr.split(/\r?\n/)){
  const closed=closedRehearsalDiagnostic(line,lineMap,new Set(diagnosticCodes));
  if(closed)errors.push(closed);
  else if(/(?:ERROR|FATAL):/.test(line))unexpectedError=true;
 }
 const starts=result.stdout.split(/\r?\n/).filter(x=>/^HR_FILE_BEGIN \d+$/.test(x)),ends=result.stdout.split(/\r?\n/).filter(x=>/^HR_FILE_END \d+$/.test(x));
 const expected=[...sql.matchAll(/^\\echo HR_FILE_BEGIN (\d+)$/gm)].map(x=>Number(x[1]));
 const gotStart=starts.map(x=>Number(x.split(' ').at(-1))),gotEnd=ends.map(x=>Number(x.split(' ').at(-1)));
 const complete=JSON.stringify(gotStart)===JSON.stringify(expected)&&JSON.stringify(gotEnd)===JSON.stringify(expected)&&!unexpectedError;
 let installedContract=null,contractFailure=null;
 const lines=result.stdout.split(/\r?\n/),contracts=lines.filter(line=>line.startsWith('HR_CONTRACT_JSON '));
 try{
  requireThat(lines.filter(line=>line==='HR_CONTRACT_BEGIN').length===1&&lines.filter(line=>line==='HR_CONTRACT_END').length===1
   &&contracts.length===1,'REHEARSAL_CONTRACT_ENVELOPE_INCOMPLETE');
  installedContract=assertInstalledRehearsalContract(JSON.parse(contracts[0].slice('HR_CONTRACT_JSON '.length)),{expectedContractHash,marker});
 }catch(error){contractFailure=/^REHEARSAL_[A-Z_]+$/.test(error.message)?error.message:'REHEARSAL_CONTRACT_JSON_REFUSED';}
 return {sameSession:true,explicitRollback:result.stdout.split(/\r?\n/).includes('HR_EXPLICIT_ROLLBACK_COMPLETED'),
  exitCode:result.code,errorCollectionComplete:complete,errors,
  unexpectedDiagnostic:unexpectedError?'UNMAPPED_DATABASE_ERROR':null,sqlHash,unitMarkers:expected.length,installedContract,contractFailure};
}
export const identityProbeSql=marker=>{
 requireThat(/^hr_install1_[a-f0-9]{32}$/.test(marker??''),'REHEARSAL_MARKER_REFUSED');
 return `select jsonb_build_object('physicalDatabase',current_database(),'identityTest',exists(select 1 from private.cloudtms_database_identity where singleton and environment='TEST' and customer_key is null),
 'customerKeyNull',exists(select 1 from private.cloudtms_database_identity where singleton and customer_key is null),
 'latestRelease',(select jsonb_build_object('status',status,'commit',git_commit) from private.cloudtms_database_releases order by started_at_utc desc,release_id desc limit 1),
 'module',(select active_owner||':'||owner_epoch::text from private.bpay_next_module_control where id=1),
 'applyingCount',(select count(*) from private.cloudtms_database_releases where status='APPLYING'),'markerAbsent',to_regclass(${literal('private.'+marker)}) is null)::text;`;
};

// Root obtains this finite relation list from its fresh TEST catalogue while
// holding the turn. A saved old contract cannot select or waive a table here.
// All row material stays in PostgreSQL; the result contains only digests/counts.
export function restorationSnapshotSql({identity,marker,relations,exportContractSql}){
 assertTurnIdentity(identity);
 requireThat(Array.isArray(relations)&&relations.length>0&&typeof exportContractSql==='string','REHEARSAL_SNAPSHOT_INPUT_REQUIRED');
 const exportQuery=exporterQuery(exportContractSql);
 const names=new Set(),chunks=[];
 for(const row of relations){
  requireThat(['public','private'].includes(row.schema)&&/^[a-z_][a-z0-9_]*$/.test(row.name)&&['r','p'].includes(row.kind),'REHEARSAL_SNAPSHOT_RELATION_REFUSED');
  const name=row.schema+'.'+row.name;requireThat(!names.has(name),'REHEARSAL_SNAPSHOT_RELATION_DUPLICATE');names.add(name);
  requireThat(name!=='private.'+marker,'REHEARSAL_SNAPSHOT_MARKER_ALREADY_PRESENT');
  if(/^private\.cloudtms_(?:database_identity|database_releases|migration_ledger|repeatable_ledger)$/.test(name))continue;
  chunks.push(`select ${literal(name)} relation,count(*) row_count,
   encode(pg_catalog.sha256(pg_catalog.convert_to(coalesce(string_agg(row_hash,'|' order by row_hash),''),'UTF8')),'hex') row_fingerprint
   from (select encode(pg_catalog.sha256(pg_catalog.convert_to(to_jsonb(t)::text,'UTF8')),'hex') row_hash from "${row.schema}"."${row.name}" t) hashed_rows`);
 }
 requireThat(chunks.length>0,'REHEARSAL_SNAPSHOT_BUSINESS_RELATIONS_REQUIRED');
 const hash=value=>`encode(pg_catalog.sha256(pg_catalog.convert_to((${value})::text,'UTF8')),'hex')`;
 const identityQuery=identityProbeSql(marker).trim().replace(/;\s*$/,'');
 return `set local search_path = pg_catalog, public;\nwith identity_proof as (${identityQuery}), business as (${chunks.join('\nunion all\n')}),
 ledger as (select jsonb_build_object('identity',(select coalesce(jsonb_agg(to_jsonb(l)),'[]'::jsonb) from private.cloudtms_database_identity l),
 'migrations',(select coalesce(jsonb_agg(to_jsonb(l) order by path),'[]'::jsonb) from private.cloudtms_migration_ledger l),
 'repeatables',(select coalesce(jsonb_agg(to_jsonb(l) order by path),'[]'::jsonb) from private.cloudtms_repeatable_ledger l),
 'releases',(select coalesce(jsonb_agg(to_jsonb(l) order by release_id),'[]'::jsonb) from private.cloudtms_database_releases l)) value),
 catalogue as (${exportQuery})
 select (identity_proof.jsonb_build_object::jsonb || jsonb_build_object(
 'ledgerFingerprint',${hash('select value from ledger')},
 'catalogueFingerprint',${hash('select to_jsonb(c) from catalogue c')},
 'businessCountsFingerprint',${hash("select coalesce(jsonb_agg(jsonb_build_object('relation',relation,'count',row_count) order by relation),'[]'::jsonb) from business")},
 'businessRowsFingerprint',${hash("select coalesce(jsonb_agg(jsonb_build_object('relation',relation,'hash',row_fingerprint) order by relation),'[]'::jsonb) from business")},
 'relationCensus',${literal(digest(JSON.stringify([...names].sort())))},
 'otherActivityCount',(select count(*) from pg_catalog.pg_stat_activity where datname=current_database() and ${ownTurnActivityExclusionSql(identity)} and (state <> 'idle' or xact_start is not null))))::text from identity_proof;`;
}

if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 throw Error('SOURCE_ONLY_ENTRY_ROOT_NATIVE_CALLBACK_REQUIRED');
}
