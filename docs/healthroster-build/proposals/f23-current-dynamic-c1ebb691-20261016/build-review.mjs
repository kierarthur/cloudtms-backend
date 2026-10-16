// Durable source evidence only. git-show/read-only inputs; writes this proposal.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import {createHash}from'node:crypto';import{execFileSync}from'node:child_process';import{fileURLToPath,pathToFileURL}from'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(dir,'../../../..'),root=path.resolve(own,'../healthroster-install2-14102026');
const commit='c1ebb691feb6a11324060e0c28210e2bd735624a',hash=x=>createHash('sha256').update(x).digest('hex');
const get=(ref,file)=>execFileSync('git',['show',`${ref}:${file}`],{cwd:root,maxBuffer:25e6}).toString();
const frozen=file=>get(commit,file),write=(file,data)=>fs.writeFileSync(path.join(dir,file),JSON.stringify(data,null,2)+'\n');
const extraction=JSON.parse(fs.readFileSync(path.join(dir,'EXTRACTION.json'))),ast=JSON.parse(fs.readFileSync(path.join(dir,'AST.json')));
assert.equal(extraction.sourceCommit,commit);assert.equal(ast.sourceCommit,commit);assert.equal(ast.counts.actualDynamicExecuteStatements,100);
const oldBytes=fs.readFileSync(path.resolve(own,'../healthroster-v2-money-build-13102026/.codex-tmp/f23-uuid-review/dynamic/REVIEW.json'));
assert.equal(hash(oldBytes),'ec6085b5746ef5542c38c1cb4624b04e86040ba66aef3a82e45f82b0d2de631a');const prior=JSON.parse(oldBytes);
fs.writeFileSync(path.join(dir,'PRIOR_SOURCE_REVIEW.json'),oldBytes);
const {specialConstructViolations,executableParts}=await import(pathToFileURL(path.join(root,'scripts/bpay-next-source-gates.mjs')));
const {scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')));
const counterparts=[];
for(const [n,ref,file,expected]of[
 [41,'c86a1dfcf0098a26b6f0eac085c95c9b08f81074','docs/healthroster-build/proposals/v6-invoice-native-20261016/forward/070_generation_native.sqlinc','c8524ad717b322439f767c70ec46e038645878580157d1037c89f4f93400563f'],
 [42,'975376fd97855248c76b0ccbe027eed9b50f25ff','docs/healthroster-build/proposals/candidate-native-pending-20261016/forward/10_native_workflow_admission.sqlinc','3f9f6fbde4afe49b79c52317a1b5307c0c9de47572da1d534c30b42dff12c41b']]){
 const producer=extraction.bodies[n-1],source=get(ref,file);assert.equal(hash(source),expected);
 const whole=fs.readFileSync(path.join(dir,producer.producerFile),'utf8');assert.equal(whole,source.trim());
 const parts=executableParts(source,producer.file).filter(p=>p.signature===producer.signature);assert.equal(parts.length,1);assert.equal(hash(parts[0].text.trim()),producer.sha256);
 const points=extraction.groups.filter(g=>g.file===producer.file&&g.signature===producer.signature).flatMap(g=>g.points);
 const actual=specialConstructViolations(source,producer.file).filter(p=>p.hazard==='DYNAMIC_SQL_UNCLASSIFIED');
 for(const point of points)assert(actual.some(p=>hash(p.key)===point.keySha256));
 counterparts.push({sourceCommit:ref,sourcePath:file,sourceFileSha256:expected,signature:producer.signature,completeStatementSha256:hash(whole),bodySha256:producer.sha256,completeStatementEqual:true,bodyEqual:true,reportedStatementKeysEqual:true,pointKeySha256:points.map(p=>p.keySha256),normalization:'Only one trailing LF removed by complete-statement scanner; no body edit, alias rewrite or approval borrowing.'});
}
const candidateFile='supabase/repeatable/16102026_1003_nhsp_v6_candidate_native_authority/01_final_functions.sqlinc';
const candidateJoined=get('f5668bf45854ca36b47e713c0e8f3dd3ae4b852b','docs/healthroster-build/proposals/candidate-joined-final-20261016/forward/CANDIDATE_FINAL_FUNCTIONS.sqlinc');
assert.equal(frozen(candidateFile),candidateJoined);
counterparts[1].joinedFrozenCommit='f5668bf45854ca36b47e713c0e8f3dd3ae4b852b';counterparts[1].joinedFileEqual=true;counterparts[1].joinedFileSha256=hash(candidateJoined);
const dependencyAmendments=[];
const dependencies=prior.callee_and_constructor_inputs.map(p=>{const s=frozen(p.file);let reason='Exact previous full callee/constructor/schema input independently re-read at c1ebb; preserved complete producer authority, not a native qualification.';
 if(hash(s)!==p.sha256){
  assert.equal(p.file,'supabase/repeatable/13102026_1134_weekly_source_rule_sets_v1.sql');
  const previous=get(prior.census_source_commit,p.file);assert.equal(hash(previous),p.sha256);
  const before="v_apply_values:=v_entry->'before_values'||jsonb_build_object('effective_from',v_scope_date);",after="v_apply_values:=(v_entry->'before_values')||jsonb_build_object('effective_from',v_scope_date);";
  assert.equal(previous.split(before).length,2);assert.equal(previous.replace(before,after),s);
  reason='Exact sole c1ebb amendment explicitly groups JSONB before_values extraction before JSONB concatenation in static settings rollback. No EXECUTE/template/identifier/USING/callee or permission edit; current complete file pinned, no claim of old/new runtime equivalence.';
  dependencyAmendments.push({path:p.file,previousSha256:p.sha256,currentSha256:hash(s),before,after,reason});
 }
 return{path:p.file,sha256:hash(s),reason};});
const additional=[
 'supabase/repeatable/30082026_1232_candidate_qr_document_revision_order_v1.sql',
 'supabase/repeatable/16102026_0858_nhsp_v6_daily_native_authority_v1.sql',
 ...['01_context.sqlinc','02_admission.sqlinc','03_requests.sqlinc','04_final_acl.sqlinc'].map(x=>'supabase/repeatable/16102026_0858_nhsp_v6_daily_native_authority/'+x),
 'supabase/repeatable/16102026_0955_nhsp_v6_invoice_native_authority_v1.sql',
 'supabase/repeatable/16102026_1003_nhsp_v6_candidate_native_authority_v1.sql',
 'supabase/repeatable/16102026_1003_nhsp_v6_candidate_native_authority/03_final_acl.sqlinc',
 'docs/healthroster-build/v6-owner-delivery-20261016/INVOICE_NATIVE_SQL_SOURCE_INTEGRATION.json',
 'docs/healthroster-build/v6-owner-delivery-20261016/CANDIDATE_NATIVE_SQL_SOURCE_INTEGRATION.json',
 'docs/healthroster-build/v6-owner-delivery-20261016/INVOICE_SOURCE_ASSEMBLY_20261016/ASSEMBLY_MANIFEST.json',
 'supabase/repeatable/16102026_0713_nhsp_v6_06_integrated_move_source_composition_v1.sql',
 'supabase/repeatable/includes/nhsp_v6_health_review_context.sqlinc',
 'supabase/repeatable/includes/nhsp_v6_health_native_apply.sqlinc',
 'supabase/repeatable/includes/nhsp_v6_health_native_compatibility.sqlinc',
 'supabase/repeatable/includes/nhsp_v6_health_composition_wrappers.sqlinc',
 'supabase/repeatable/includes/nhsp_v6_health_contract_final_acl.sqlinc'];
for(const file of additional)dependencies.push({path:file,sha256:hash(frozen(file)),reason:'Complete joined constructor/callee/ACL/order or source provenance input. No native/provider/financial approval.'});
const daily=frozen(additional[5]),dailyPart=executableParts(daily,additional[5])[0];
assert.equal(hash(dailyPart.text.trim()),'a6455e373908a48d6e3c66dab688096b7cb7455050b42cf70650c7eb0e861ead');
const dailySignatures=[...daily.matchAll(/^\s*'(private\.[^']+)'[,\s]*$/gm)].map(x=>x[1]);assert.equal(dailySignatures.length,52);assert.equal(new Set(dailySignatures).size,52);
const dailyAll=[...additional.slice(2,5),...additional.slice(-5)].map(frozen).join('\n');for(const signature of dailySignatures)assert(executableParts(dailyAll,'daily-definitions').some(p=>p.signature===signature),signature);
// These are the three raw dynamic keys already inherited by the existing
// source gate, distinct from this proposal's 99 changed diagnostics.
const frozenBase='a6f7423eb71e6e7cd173e705c2290e5408a1d077',inherited=[];
const expectedInherited=[['supabase/repeatable/04092026_1952_candidate_expense_history_anchor_recovery_v1.sql',candidateFile],['supabase/repeatable/21072026_1235_59_invoice_issue_one.sql','supabase/repeatable/16102026_0955_nhsp_v6_invoice_native_authority/01_complete_invoice_authorities.sqlinc']];
for(const [oldFile,newFile]of expectedInherited){const previous=get(frozenBase,oldFile),previousPoints=specialConstructViolations(previous,oldFile).filter(x=>x.hazard==='DYNAMIC_SQL_UNCLASSIFIED');
 for(const p of specialConstructViolations(frozen(newFile),newFile).filter(x=>x.hazard==='DYNAMIC_SQL_UNCLASSIFIED')){
  const reported=extraction.groups.filter(g=>g.file===newFile).flatMap(g=>g.points).some(x=>x.keySha256===hash(p.key));if(reported)continue;
  assert(previousPoints.some(x=>x.key===p.key));const [signature,,statement]=JSON.parse(p.key);
  const producer=executableParts(frozen(newFile),newFile).filter(x=>x.signature===signature&&x.text.includes(statement));assert.equal(producer.length,1);
  inherited.push({file:newFile,signature,keySha256:hash(p.key),statementSha256:hash(statement),bodySha256:hash(producer[0].text.trim()),frozenBase,frozenFile:oldFile,frozenFileSha256:hash(previous),keyEqual:true,admission:'EXISTING_FROZEN_BASELINE_ONLY_NO_NEW_ALLOWANCE'});
 }
}assert.equal(inherited.length,3);
const inheritedInvoiceFile='supabase/repeatable/16102026_0955_nhsp_v6_invoice_native_authority/01_complete_invoice_authorities.sqlinc',inheritedInvoiceSource=frozen(inheritedInvoiceFile);
const inheritedInvoice=scanManagedSql(inheritedInvoiceSource).statements.map(s=>inheritedInvoiceSource.slice(s.start,s.end)).filter(s=>executableParts(s,inheritedInvoiceFile).some(p=>p.signature==='public.invoice_issue_one(uuid,uuid)'));
assert.equal(inheritedInvoice.length,1);fs.writeFileSync(path.join(dir,'INHERITED_INVOICE_ISSUE_COMPLETE.sqlinc'),inheritedInvoice[0]);
write('INHERITED_PRODUCER.json',{file:inheritedInvoiceFile,signature:'public.invoice_issue_one(uuid,uuid)',producerFile:'INHERITED_INVOICE_ISSUE_COMPLETE.sqlinc',producerSha256:hash(inheritedInvoice[0]),purpose:'Raw35-file coverage only; two existing frozen baseline points, no new allowance.'});
let rawFilePoints=0;for(const f of extraction.files)rawFilePoints+=specialConstructViolations(frozen(f.file),f.file).filter(x=>x.hazard==='DYNAMIC_SQL_UNCLASSIFIED').length;assert.equal(rawFilePoints,102);
const additions={
 DAILY:{kind:'CLOSED_LITERAL_OWNER_ONLY_ACL_REGISTRY',provenance:'Exactly52 literal private signatures in FOREACH signature; signature has no other assignment, query/caller/table input. to_regprocedure must resolve before each statement. %s is exclusively that finite literal signature; CURRENT_USER is only passed as quoted %I.',effects:'ALTER FUNCTION OWNER to actual CURRENT_USER and REVOKE ALL from PUBLIC, anon, authenticated, service_role. Two actual DDL statements, no body invocation/business DML/service grant. All52 full native definitions and include/ACL order pinned.',checks:'Complete DO and complete same closure definitions bound. Missing function raises CONTRACT_MOVE_ACL_DEFINITION_MISSING. Actual native owner/ACL/exposure proof remains NOT_RUN.'},
 INVOICE:{kind:'CONSTANT_NATIVE_CREDIT_CTE_EXECUTION_IN_JOINED_OWNER',provenance:'Exact complete CREATE FUNCTION equals frozen c86a 070_generation_native; whole body ec83 is independently pinned, not borrowed from older d8d7/6e3c owners. Exact c491 statement and9511-byte constant query equal both old credit counterparts. Typed p_claims JSONB/v_now timestamptz are the only $1/$2 USING values.',effects:'Real credit financial DML:8 CTE writes on6relations, then original static document/issue queue owners. No dynamic identifier or command input. Full current joined94function file, actual source-original entry lock and financial owner guards remain whole.',checks:'Original claim admission enters before the pipeline; actual source invoices locked ordered FOR UPDATE before dynamic query. Stored chunk/source invoice type,status,resume operation,revision and correction failures constrain creation. This source construction classification grants no invoice/Bank/policy authority and is not native financial closure.'},
 CANDIDATE:{kind:'LITERAL_EFFECT_CALL_IN_EXACT_JOINED_WORKFLOW_OWNER',provenance:'Complete263481-byte CREATE FUNCTION equals frozen975 native pending body and exact joined f566 file. New IF branch expression/whole body are pinned separately. Literal public.candidate_targetless_expense_paper_pack_enqueue_v1($1..$5); environment text, workflow UUID, generation integer, idempotency text and time timestamptz are data-only USING values.',effects:'Real PAPER preparation/enqueue effect. Current callee can INSERT mail_outbox; ordinary inherited ELSE additionally uses real document/QR/financial-state enqueue owner. This is not an inert/read-only call and never authorizes running it.',checks:'Whole owner retains session-selected account/Candidate scope, feature gates, exact generation, immutable receipt replay, workflow locks, genuine move-source lock/revalidation, eligible state, route policy, immutable component manifest. Current callee rechecks workflow/generation/manifest/anchor/reserved week/Candidate recipient opt-in/conflict/lease; queues held empty-attachment mail only. Caller requires ok/queued/recipient, nonnull UUID receipt and locks exact mail identity/generation/manifest/held flags/status/emptyattachments before returning. Native API/provider/race/role proof remains NOT_RUN.'}
};
const rows=extraction.groups.map(g=>{
 const old=prior.rows.find(r=>r.file===g.file&&r.signature===g.signature);
 let review=old?.review;if(!review)review=g.file.includes('0858_')?additions.DAILY:g.file.includes('0955_')?additions.INVOICE:additions.CANDIDATE;
 return{...g,review,points:g.points.map(p=>({...p,disposition:g.file.includes('13102026_1430_')?'WITHHELD_1430_EXACT_PREBODY_NATIVE_PREREQUISITE':g.file.includes('14102026_0400_')?'WITHHELD_0400_ACTUAL_PENDING_PREFIX_NATIVE_CENSUS':'FINITE_EXECUTION_CONSTRUCTION_ONLY',priorAssessment:old?.points.find(x=>x.statement_sha256===p.statementSha256)?.assessment??null}))};
});
const result={version:'HEALTHROSTER_CURRENT_DYNAMIC_SOURCE_REVIEW_V1',status:'SOURCE_EXECUTION_CONSTRUCTION_ONLY_NATIVE_NOT_RUN',sourceCommit:commit,censusSha256:extraction.censusSha256,priorReviewSha256:hash(oldBytes),
 counts:{...extraction.counts,reportedActualExecuteStatements:99,selectedCompleteProducerActualExecuteStatements:100,all35FilesRawDynamicKeys:102,all35FilesCompleteDynamicProducers:45,inheritedFrozenKeys:3,proposedFinitePoints:95,withheld1430:3,withheld0400:1},
 boundary:'Exact source SQL construction review only. Existing writer, context, ledger, policy, monetary, ACL, publication, schema/type, native-callee, fixture and rollback gates retain independent force. A source classification never permits executing a financial/provider/email action or validates its business outcome.',
 unresolved:[{file:'supabase/migrations/13102026_1430_weekly_source_invoice_money_origins.sql',points:3,reason:'Exact predecessor source set and complete LOCK+prebody proposal exist at adbb, but c1ebb has no pinned actual integrated companion receipt. No allowance here. Integrator must bind actual compiler/harness/order/closure/program/native qualification independently.'},{file:'supabase/migrations/14102026_0400_healthroster_protected_source_capability.sql',points:1,reason:'Exact native constraint names/deparse after actual managed pending1133 prefix still unknown. Native relation absent in root fresh catalogue; source-only expression or guessed name is not census. No allowance.'},
 {reason:'Chosen-detail Banking producer seam and current native role/ACL/provider/financial/race/API/rollback tests remain separate prerequisites. Finite read SQL cannot fabricate a producer.'}],
 counterparts,inheritedFrozenPoints:inherited,dailyLiteralSignatures:dailySignatures,dependencies,dependencyAmendments,
 predecessorEvidence:{sourceProposalCommit:'adbb0cb4d3a5269a0de727ae50fcb4cdb427a5ec',sourceReviewPath:'docs/healthroster-build/proposals/f23-native-predecessor-review-20261016/REVIEW.json',sourceReviewSha256:'10573f03cf4d6667627efe08d9e614a8a71d18d77dab9b70f259bf006a688799',native:'NOT_RUN'},
 files:extraction.files,producers:extraction.bodies,rows,analysisToolSourcePins:extraction.analysisToolSourcePins,astSha256:hash(fs.readFileSync(path.join(dir,'AST.json'))),native:'NOT_RUN',databaseCalls:0};
write('REVIEW.json',result);
const mountBase='docs/healthroster-build/v6-owner-delivery-20261016/F23_DYNAMIC_SOURCE_REVIEW_20261016';
const data={version:'HEALTHROSTER_EXACT_DYNAMIC_CONSTRUCTION_CLASSIFICATION_V1',status:result.status,sourceCommit:commit,native:'NOT_RUN',review:{path:mountBase+'/REVIEW.json',sha256:hash(fs.readFileSync(path.join(dir,'REVIEW.json')))},priorReview:{path:mountBase+'/PRIOR_SOURCE_REVIEW.json',sha256:hash(oldBytes)},ast:{path:mountBase+'/AST.json',sha256:result.astSha256},dependencies,
 counts:result.counts,files:extraction.files.map(f=>({path:f.file,sha256:f.sha256,rawDynamicKeys: specialConstructViolations(frozen(f.file),f.file).filter(x=>x.hazard==='DYNAMIC_SQL_UNCLASSIFIED').map(x=>({keySha256:hash(x.key),count:1})),points:rows.filter(g=>g.file===f.file).flatMap(g=>g.points.filter(p=>p.disposition==='FINITE_EXECUTION_CONSTRUCTION_ONLY').map(p=>({signature:g.signature,keySha256:p.keySha256,statementSha256:p.statementSha256,bodySha256:p.bodySha256,count:1,reviewKind:g.review.kind})))})).filter(f=>f.points.length)};
assert.equal(data.files.reduce((n,f)=>n+f.points.length,0),95);write('CLASSIFICATION.json',data);
console.log(JSON.stringify({counts:result.counts,reviewSha256:data.review.sha256,classificationSha256:hash(fs.readFileSync(path.join(dir,'CLASSIFICATION.json'))),counterparts}));
