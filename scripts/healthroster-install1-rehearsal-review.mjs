// Source-only review record writer. It never executes SQL or reads a secret.
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {canonical,digest,scanManagedSql} from './cloudtms-managed-release-sql.mjs';
import {mappedVerifier,REHEARSAL_VERSION} from './healthroster-install1-full-rehearsal.mjs';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const exclusions=new Map([
 ['tests/29082026_0117_candidate_daily_projection_expired_lease_recovery_runtime_verification.sql','ACTUAL_QUEUE_PROCESSING: calls candidate_daily_projection_claim_v1 with a global target, recovering/claiming genuine hosted projection rows before fixture completion.'],
 ['tests/31082026_1437_banking_pay_candidate_group_pagination_runtime_verification.sql','GENUINE_APPLYING_RELEASE_REQUIRED: includes tests/fixtures/28082026_1429_banking_pay_selection_setup.sql, requiring exactly one genuine unfinished APPLYING release with its actual identity/commit/mode. A rollback rehearsal must not fabricate it.'],
 ['tests/31082026_1907_banking_pay_candidate_group_display_only_selection_tuple_runtime_verification.sql','GENUINE_APPLYING_RELEASE_REQUIRED: same exact selection setup include and genuine unfinished APPLYING identity/commit/mode gate. Read owners are otherwise harmless.'],
 ['tests/01092026_1511_banking_pay_signed_recovery_draft_runtime_verification.sql','GENUINE_APPLYING_RELEASE_REQUIRED_AND_PAYMENT_LIFECYCLE: exact selection setup requires a genuine APPLYING release; the runtime body also calls pay_batch_finalize_reservations_and_markers on a Draft fixture.'],
 ['tests/02092026_2392_banking_pay_workbench_settled_certificate_v8_runtime_verification.sql','ACTUAL_QUEUE_PROCESSING: executes settled certificate due_claim/build_start/append/seal and certified Draft operation_start, rather than inspecting their definitions.'],
 ['supabase/verification/06092026_2358_candidate_advanced_expense_policy_verification.sql','ACTUAL_QUEUE_PROCESSING: calls email_outbox_claim_ready_batch and candidate_expense_summary_claim_v1/render_begin/complete. SQL rollback does not authorise these global queue lifecycles.'],
 ['supabase/verification/15092026_1534_weekly_source_query_delivery_v1.sql','ACTUAL_QUEUE_PROCESSING: pg_temp claim loops invoke weekly_source_message_dispatch_claim_v1 and target_claim_v1, which can lease hosted rows before filtering to fixtures.'],
 ['supabase/verification/15092026_1534_weekly_source_ordinary_pay_projection_v1.sql','ACTUAL_QUEUE_PROCESSING: paid-fixture preparation invokes pay_workbench_dirty_apply_jobs_chunk, the real job claim/apply processor.'],
 ['supabase/verification/15092026_1534_weekly_source_correct_final_source_v1.sql','ACTUAL_QUEUE_PROCESSING_THROUGH_INCLUDE: expands the complete ordinary pay projection verifier, which invokes pay_workbench_dirty_apply_jobs_chunk.'],
 ['supabase/verification/15092026_1534_weekly_source_invoice_admission_v1.sql','ACTUAL_QUEUE_PROCESSING_THROUGH_INCLUDE: recursively expands ordinary pay projection/correct final source verifiers and their actual dirty job processor.'],
 ['supabase/verification/15092026_1534_weekly_source_read_projections_v1.sql','ACTUAL_QUEUE_PROCESSING: executed paid-fixture preparation invokes pay_workbench_dirty_apply_jobs_chunk.'],
 ['supabase/verification/02092026_1833_weekly_source_invoice_issue_validator_v1.sql','ACTUAL_QUEUE_PROCESSING_THROUGH_INCLUDE: recursively expands ordinary pay projection/invoice admission verifiers and their actual dirty job processor.'],
 ['supabase/verification/17092026_0700_weekly_source_pending_entitlement_release_v1.sql','ACTUAL_QUEUE_PROCESSING: invokes pending entitlement release claim page/apply, whose watch/claim lifecycle leases native PENDING/RELEASING bundles using SKIP LOCKED.'],
 ['supabase/verification/17092026_1100_weekly_source_candidate_view_producer_v1.sql','ACTUAL_QUEUE_PROCESSING: executed paid-fixture preparation invokes pay_workbench_dirty_apply_jobs_chunk.'],
 ['supabase/verification/17092026_1200_weekly_source_audit_and_export_v1.sql','ACTUAL_QUEUE_PROCESSING: executed paid-fixture preparation invokes pay_workbench_dirty_apply_jobs_chunk. The separate pg_temp drain helper is fixture DML, but cannot legitimise the actual processor call.'],
 ['supabase/verification/19092026_1645_weekly_source_completed_pack_copy_v1.sql','ACTUAL_QUEUE_PROCESSING: executes email_outbox_claim_ready_batch for copy probes; fixture ordering/sentinels do not confine the generic mail owner.'],
]);

export function buildSafetyReview(census,{sourcePin}={}){
 if(census.sourceCounts.verifiers!==158||!/^[0-9a-f]{40}$/.test(sourcePin??''))throw Error('INSTALL1_REVIEW_IDENTITY_REFUSED');
 const verifiers=census.verifiers.map(row=>{
  const {sql,original}=mappedVerifier(row.file),parsed=scanManagedSql(sql);
  if(row.sourceSqlHash!==original.sourceSqlHash||row.closureHash!==original.closureHash)throw Error('INSTALL1_REVIEW_SOURCE_DRIFT');
  const commands=[...parsed.meta.filter(x=>/^\\set\s+ON_ERROR_(?:STOP|ROLLBACK)\b/i.test(x.text)).map(x=>({...x,command:x.text.trim()})),
   ...parsed.statements.filter(x=>['BEGIN','COMMIT','ROLLBACK','END','START','ABORT','PREPARE'].includes(x.tokens[0])&&!(x.tokens[0]==='ROLLBACK'&&x.tokens[1]==='TO')).map(x=>({...x,command:x.tokens.join(' ')}))].sort((a,b)=>a.start-b.start);
  const excluded=exclusions.has(row.file);
  // Risk detection validates the separately reviewed closed exclusion list.
  // A newly introduced real effect stops this writer; it is never auto-included.
  if((row.prohibitedCandidates.length||row.applyingReferences)&&!excluded)throw Error('INSTALL1_NEW_EFFECT_REQUIRES_REVIEW '+row.file);
  const catalogueOnly=row.writes.length===0&&row.callees.length===0;
  return {index:row.index,file:row.file,decision:excluded?'EXCLUDE':'INCLUDE',
   reason:exclusions.get(row.file)??(catalogueOnly?'Complete installed definition, security, ACL, shape and effective-privilege assertions only; no application owner is invoked.':
    row.index===157?'Complete WP-PP fixture actor matrix and reduced/combined PAY read first use. Direct owners are pay_actor_allowed, office_authority, office_workspace and combined_review_workspace; current source hashes and trigger effects are captured. No protected publication, Draft, payment, provider, communications sender or queue claim is called.':
    'Complete native fixture/census body retained. Explicit application/read/authority calls, source-owned trigger effects, fixture DML and dynamic assertion helpers were censused. No payment/provider/settlement execution, external sender or actual queue processor is invoked; fixture state is rolled back to its per-verifier savepoint.'),
   sourceSqlHash:row.sourceSqlHash,closureHash:row.closureHash,executableSqlHash:digest(sql),effectGraphHash:row.effectGraphHash,
   completeClosureFiles:row.files,expandedBytes:row.bytes,statementCount:row.statements,
   removedCommands:commands.map(x=>x.command),readOnlyBodyReviewed:row.envelopes.includes('BEGIN READ ONLY'),
   nativeIsolationRequirements:row.envelopes.filter(x=>x.startsWith('BEGIN ISOLATION LEVEL ')),
   explicitCalls:row.calls,calleeKeys:row.callees,prohibitedCalls:row.prohibitedCandidates,
   genuineApplyingReleaseRequired:row.applyingReferences,sequenceGapPossible:row.sequenceCalls,
   unresolvedLexicalCandidates:row.unknownCalls,
   assertionBodyDisposition:'KEEP_COMPLETE',fixtureDisposition:excluded?'DO_NOT_EXECUTE':'SAVEPOINT_AND_ROLLBACK',
   pendingIndependentReview:false};
 });
 const sourceFiles=new Map();
 for(const row of census.verifiers)for(const file of row.files)sourceFiles.set(file.file,file.sha256);
 for(const row of Object.values(census.calleeDictionary))for(const file of row.sourceFileHashes??[])sourceFiles.set(file.file,file.sha256);
 return {version:REHEARSAL_VERSION,status:'REVIEWED_SOURCE_ONLY_NOT_NATIVE_PROOF',sourcePin,
  sqlRuntimeSourcePin:sourcePin,sourceCounts:census.sourceCounts,reviewed:verifiers.length,
  included:verifiers.filter(x=>x.decision==='INCLUDE').length,excluded:verifiers.filter(x=>x.decision==='EXCLUDE').length,
  calleeDictionarySha256:digest(JSON.stringify(census.calleeDictionary)),sourceFiles:[...sourceFiles].sort(([a],[b])=>a.localeCompare(b)).map(([file,sha256])=>({file,sha256})),
  newUpgradePpVerifierParity:census.newVerifierParity,nativeFull:'NOT_RUN',mutationsAuthorised:false,
  lexicalLimit:'Callee dictionary is a conservative potential source graph, including trigger branches and every overload. It does not prove a live catalogue or infer that every branch executes. Dynamic/catalogue literal candidates and renamed/builtin nodes need the documented source dispositions; root binds fresh catalogue before native work.',
  archivedHistory:'168/152/16 and 172-root-cause source reviews in the broad 13102026 rehearsal history remain untouched. Their old pins and fingerprint vectors confer no current execution authority.',
  verifiers};
}

if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER='CURRENT_USER';
 const folder=path.join(root,'docs/healthroster-build/install1-rehearsal');
 const census=JSON.parse(fs.readFileSync(path.join(folder,'VERIFIER_CALLEE_CENSUS.json'),'utf8'));
 const review=buildSafetyReview(census,{sourcePin:process.argv[2]});
 fs.writeFileSync(path.join(folder,'VERIFIER_SAFETY_REVIEW.json'),JSON.stringify(review,null,2)+'\n');
 console.log(JSON.stringify({status:review.status,reviewed:review.reviewed,included:review.included,excluded:review.excluded,nativeFull:review.nativeFull}));
}
