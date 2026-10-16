// Declared-source CREATE-time dependency review; no PostgreSQL/catalogue claims.
import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {spawnSync} from 'node:child_process';
import {fileURLToPath,pathToFileURL} from 'node:url';
import assert from 'node:assert/strict';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..');
assert(process.argv[2],'READ_ONLY_INTEGRATOR_ROOT_REQUIRED');const root=path.resolve(process.argv[2]);
const {scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')));
const {routineDefinitions}=await import(pathToFileURL(path.join(root,'scripts/bpay-next-final-definers.mjs')));
const sha=value=>createHash('sha256').update(value).digest('hex');
const lf=value=>value.toString('utf8').replaceAll('\r\n','\n');
const git=(cwd,args)=>{const r=spawnSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{cwd,maxBuffer:4_000_000,windowsHide:true});assert.equal(r.status,0,r.stderr.toString());return r.stdout;};
const candidateCommit='f5668bf45854ca36b47e713c0e8f3dd3ae4b852b';
const candidatePath='docs/healthroster-build/proposals/candidate-joined-final-20261016/REVIEWED_SELECTION.json';
const candidateBytes=git(repo,['show',`${candidateCommit}:${candidatePath}`]),candidate=JSON.parse(candidateBytes);
assert.equal(candidate.functions.length,55);
const joined=fs.readFileSync(path.join(here,'JOINED_INVOICE_SOURCE.sqlinc'),'utf8');
const manifest=JSON.parse(fs.readFileSync(path.join(here,'ASSEMBLY_MANIFEST.json')));
const sql=[],pending=[];
const functionCalls=text=>[...new Set([...text.matchAll(/\b(?:private|public)\.[a-z_][a-z0-9_]*\s*\(/gi)].map(match=>match[0].replace(/\s*\($/,'').toLowerCase()))];
for(const parsed of scanManagedSql(joined).statements){
 const statement=joined.slice(parsed.start,parsed.end).trim();
 const [signature]=routineDefinitions(statement,'JOINED_INVOICE_SOURCE.sqlinc');if(!signature)continue;
 const header=statement.slice(0,statement.indexOf('$function$'));
 const calls=functionCalls(statement).filter(name=>name!==signature.split('(')[0]);
 const selectedCalls=candidate.functions.filter(row=>calls.includes(row.signature.split('(')[0]));
 const origin=manifest.functions.find(row=>row.signature===signature);
 const row={signature,statementSha256:sha(statement),source:origin.source,
  language:/\blanguage\s+sql\b/i.test(header)?'SQL':'PLPGSQL',
  candidateCalls:selectedCalls.map(row=>({signature:row.signature,sourceCommit:row.commit,path:row.path,spanSha256:row.spanSha256})),
  allQualifiedCalls:calls};
 if(row.language==='SQL')sql.push(row);
 if(selectedCalls.some(row=>/\.timesheet_contract_native_(?:pending_|rotation_binding_|allocation_binding_)/.test(row.signature)))pending.push(row);
}
assert.equal(sql.length,10);assert.equal(pending.length,2);assert(pending.every(row=>row.language==='PLPGSQL'));
assert(sql.every(row=>!row.candidateCalls.some(call=>call.signature.includes('timesheet_contract_native_pending'))));
const sqlCandidateCalls=sql.flatMap(row=>row.candidateCalls);
assert.equal(sqlCandidateCalls.length,1);
assert.equal(sqlCandidateCalls[0].signature,'private.timesheet_contract_move_evidence_source_v1(uuid)');
const coreRootPath='supabase/repeatable/16102026_0713_nhsp_v6_06_integrated_move_source_composition_v1.sql';
const corePath='supabase/repeatable/includes/nhsp_v6_health_native_apply.sqlinc';
const coreRoot=lf(fs.readFileSync(path.join(root,coreRootPath))),core=lf(fs.readFileSync(path.join(root,corePath)));
assert(coreRoot.includes('\\ir includes/nhsp_v6_health_native_apply.sqlinc'));
const coreDefinition=scanManagedSql(core).statements.map(row=>core.slice(row.start,row.end).trim())
 .find(statement=>routineDefinitions(statement,corePath).includes(sqlCandidateCalls[0].signature));
assert(coreDefinition);assert.match(coreDefinition,/returns jsonb\s+language plpgsql/i);
const finalEvidence=candidate.functions.find(row=>row.signature===sqlCandidateCalls[0].signature);
const finalBytes=git(repo,['show',`${finalEvidence.commit}:${finalEvidence.path}`]);
assert.equal(sha(finalBytes),finalEvidence.fileSha256);
const finalSpan=finalBytes.subarray(finalEvidence.startByte,finalEvidence.endByteExclusive);
assert.equal(sha(finalSpan),finalEvidence.spanSha256);
const finalStatement=lf(finalSpan);assert.match(finalStatement,/returns jsonb\s+language plpgsql/i);
assert.deepEqual(routineDefinitions(finalStatement,finalEvidence.path),[sqlCandidateCalls[0].signature]);
const out={version:'HEALTH_INVOICE_CREATE_ORDER_SOURCE_REVIEW_V1',qualification:'DECLARED_SOURCE_NOT_NATIVE_CATALOGUE',
 assemblySha256:sha(joined),candidateSelection:{commit:candidateCommit,path:candidatePath,sha256:sha(candidateBytes),functions:55},
 sqlDefinitionCount:sql.length,sqlDefinitions:sql,pendingRuntimeCallers:pending,
 existingCreateTimeCandidateDependency:{signature:sqlCandidateCalls[0].signature,returns:'jsonb',rootCommit:git(root,['rev-parse','HEAD']).toString().trim(),
  existingRootPath:coreRootPath,existingRootCanonicalLfSha256:sha(coreRoot),existingDefinitionPath:corePath,existingFileCanonicalLfSha256:sha(core),
  existingStatementSha256:sha(coreDefinition),existingHeader:coreDefinition.slice(0,coreDefinition.indexOf('as $function$')).trim(),
  finalCandidateSource:finalEvidence,finalHeader:finalStatement.slice(0,finalStatement.indexOf('as $function$')).trim(),
  sourceAbi:'Same single named uuid input and jsonb return; existing0713 routine resolves SQL issue-validator creation before finalCandidate55.'},
 requiredCandidateFoundationPreload:[],
 order:['Canonical Root01 schema and exact earlier Physical0713 core including evidence_source(uuid)->jsonb; Daily04b/04c/04d complete authority',
  'Complete Invoice94 assembly with all retained DDL/ACL effects and final099',
  'Complete finalCandidate55 and pending one-time schema/effects/final ACL according to f566 recipe; no duplicate full package or stub preload',
  'Complete Source pending/current helper composition and exact full Invoice-duty successor in the same reviewed final composition',
  'Final security/contract and first use after all final definitions are installed; preserve feature-off/intermediate native-prefix legacy behaviour'],
 noShortcut:['No invented preload','No stub or fake prologue','No check_function_bodies=off','No duplicate finalCandidate55'],
 limits:'PLpgSQL calls are runtime dependencies, not proof of successful first use. Native PostgreSQL17 CREATE, catalogue/ACL, full first-use and concurrency acceptance remain NOT_RUN.'};
fs.writeFileSync(path.join(here,'CREATE_ORDER_REVIEW.json'),JSON.stringify(out,null,2)+'\n');
console.log(JSON.stringify({sql:sql.length,newPendingSqlCalls:0,pendingPlpgsqlCallers:pending.length,candidateFoundationPreload:0,existingCoreAbi:sqlCandidateCalls[0].signature+' -> jsonb'}));
