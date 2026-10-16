// Independent dispatch-authority source review; no DB/provider operation.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';import {spawnSync} from 'node:child_process';import {fileURLToPath,pathToFileURL} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..');
const root=path.resolve(repo,'../healthroster-install2-14102026'),money=path.resolve(repo,'../healthroster-v2-money-build-13102026');
const sourceCommit='e4661d28219b85c66b854d99c6522b55aae33139',rootCommit='efd18231c06df14ed91c7f6b137ddb6fec7e2df2';
const sourceBase='docs/healthroster-build/proposals/source-contact-producer-continuation-20261016/';
const sha=value=>createHash('sha256').update(value).digest('hex'),lf=value=>value.toString('utf8').replaceAll('\r\n','\n');
function blob(ref,file){const r=spawnSync('git',['show',`${ref}:${file}`],{cwd:repo,maxBuffer:12_000_000,windowsHide:true});assert.equal(r.status,0,r.stderr.toString());return r.stdout;}
const {scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')));
const {routineDefinitions}=await import(pathToFileURL(path.join(root,'scripts/bpay-next-final-definers.mjs')));
function defs(text,file){return scanManagedSql(text).statements.flatMap(row=>{const sql=text.slice(row.start,row.end).trim();return routineDefinitions(sql,file).map(signature=>({signature,sql,path:file,start:row.start,end:row.end,sha256:sha(sql)}));});}
const recipe=JSON.parse(blob(sourceCommit,sourceBase+'SOURCE_COMPLETION_RECIPE.json'));
const declarations=[];for(const row of recipe.assembly.repeatable){const text=lf(blob(sourceCommit,sourceBase+row.path));declarations.push(...defs(text,sourceBase+row.path));}
assert.equal(declarations.length,106);assert.equal(new Set(declarations.map(row=>row.signature)).size,106);
const prerequisitePath='docs/healthroster-build/proposals/v6-contact-managed-assembly-20261016/payload/supabase/repeatable/16102026_1209_weekly_contact_producer_prerequisites/01_complete_native_helpers.sqlinc';
const prerequisites=defs(lf(blob('2d74b1fdced14d4a60882720d7e590ad83f8dd5a',prerequisitePath)),prerequisitePath);assert.equal(prerequisites.length,5);
const all=[...prerequisites,...declarations],legacyStart='public.weekly_source_message_dispatch_submission_start_atomic_v1(jsonb)';
const legacyResult='public.weekly_source_message_dispatch_result_atomic_v1(jsonb)';
const targetStart='public.weekly_source_message_dispatch_target_start_atomic_v1(jsonb)',targetResult='public.weekly_source_message_dispatch_target_result_atomic_v1(jsonb)';
const closedPath='supabase/repeatable/15092026_2311_weekly_source_delivery_targets_v1.sql';
const nativeFile=lf(blob(rootCommit,closedPath)),native=defs(nativeFile,closedPath);
const old=declarations.find(row=>row.signature===legacyStart),closed=native.find(row=>row.signature===legacyStart),closedResult=native.find(row=>row.signature===legacyResult);
assert.equal(old.sha256,'de9458f5f3b8b6f9413c060dafcbc5c82c6af5e1d165a5167aee4c653e57c8ce');
assert.equal(closed.sha256,'3af85b27bd5a224691e41568ea0548709b7ff5eb57897abe447e275b9591b00d');
assert(old.sql.includes('insert into public.weekly_message_provider_attempts('));assert(old.sql.includes("set state='SUBMISSION_STARTED'"));
assert(!old.sql.includes('weekly_contact_submission_start_v1'));assert(!old.sql.includes('weekly_contact_scope_lock_v1'));
assert(!old.sql.includes('weekly_message_target_attempts'));assert(!old.sql.includes('weekly_contact_source_permit_v1'));
for(const row of [closed,closedResult]){
 assert.match(row.sql,/perform private\.weekly_source_query_require_service_v1\(\);\s*raise exception 'WEEKLY_SOURCE_PER_TARGET_DISPATCH_REQUIRED' using errcode='55000';/);
 assert(!/\b(insert\s+into|update|delete\s+from)\b/i.test(row.sql));
}
const writers=all.filter(row=>/\b(insert\s+into|update|delete\s+from)\s+(?:public\.)?weekly_message_(?:provider|target)_attempts\b/i.test(row.sql));
assert.deepEqual(writers.map(row=>row.signature),[legacyStart]);
for(const row of prerequisites){assert(!/\b(insert\s+into|update|delete\s+from)\b/i.test(row.sql));assert(!row.sql.includes('weekly_source_message_dispatch_submission_start_atomic_v1'));}
const protectedSignatures=[legacyStart,legacyResult,targetStart,targetResult,'public.weekly_contact_submission_start_v1(jsonb)',
 'public.weekly_contact_submission_consume_v1(jsonb)','private.weekly_contact_source_result_v1(uuid,text,text,jsonb,jsonb)',
 'private.weekly_contact_source_memberships_v1(uuid,boolean)','private.weekly_contact_source_membership_accept_v1(uuid)'];
const intersections=protectedSignatures.map(signature=>({signature,selectedDefinitions:all.filter(row=>row.signature===signature).map(({sql,...row})=>row)}));
assert(intersections.filter(row=>row.selectedDefinitions.length).length===1);
const activeStartPath='supabase/repeatable/includes/weekly_contact_source_native_start_v1.sqlinc';
const activeStart=defs(lf(blob(rootCommit,activeStartPath)),activeStartPath).find(row=>row.signature===targetStart);
const admissionAt=activeStart.sql.indexOf('public.weekly_contact_submission_start_v1('),allocateAt=activeStart.sql.indexOf('insert into public.weekly_message_target_attempts(');
assert(admissionAt>=0&&allocateAt>admissionAt);assert(activeStart.sql.includes('return private.weekly_contact_source_permit_v1('));
assert(activeStart.sql.includes("'call_once',false,'reason','CHECK_ORIGINAL_ATTEMPT'"));
const activeResult=native.find(row=>row.signature===targetResult);
assert(activeResult.sql.includes('perform private.weekly_contact_source_result_v1('));assert(activeResult.sql.includes('private.weekly_source_delivery_aggregate_command_v1('));
assert(activeResult.sql.indexOf('private.weekly_contact_source_result_v1(')<activeResult.sql.indexOf('update public.weekly_message_target_attempts'));
const wrapperPath='supabase/repeatable/16102026_0226_weekly_contact_query_outbox_admission_v1.sql';
const wrapper=defs(lf(blob(rootCommit,wrapperPath)),wrapperPath).find(row=>row.signature==='public.weekly_contact_submission_start_v1(jsonb)');
assert(wrapper.sql.includes("when 'WEEKLY_SOURCE_TARGET' then return private.weekly_contact_source_admission_v1(p_request)"));
const sourceAdmissionPath='supabase/repeatable/16102026_0214_weekly_contact_source_admission_v1.sql';
const admission=defs(lf(blob(rootCommit,sourceAdmissionPath)),sourceAdmissionPath).find(row=>row.signature==='private.weekly_contact_source_admission_v1(jsonb)');
assert(admission.sql.includes('private.weekly_contact_deployment_lock_v1('));assert(admission.sql.includes('private.weekly_contact_scope_lock_v1('));
const runtimePath='broker/src/weekly-source/delivery-runtime.mjs',runtime=lf(blob(rootCommit,runtimePath));
assert(runtime.includes("'weekly_source_message_dispatch_target_start_atomic_v1'"));assert(runtime.includes("'weekly_source_message_dispatch_target_result_atomic_v1'"));
assert(!runtime.includes("'weekly_source_message_dispatch_submission_start_atomic_v1'"));assert(!runtime.includes("'weekly_source_message_dispatch_result_atomic_v1'"));
const aclText=lf(blob(sourceCommit,sourceBase+'forward/99_acl.sqlinc'));
assert(aclText.includes('grant execute on function '+legacyStart+' to service_role;'));
const registry=JSON.parse(blob(rootCommit,'docs/bpay-next/final-definer-closures.json'));
const nativeChain=registry.chains.find(row=>row.signature===legacyStart);assert.equal(nativeChain.finalPhysical,closedPath);
assert.equal(nativeChain.finalRoot,'supabase/repeatable/28092026_1234_stage2_hosted_final_authority_reassert_v1.sql');
const currentContract=JSON.parse(blob(rootCommit,'supabase/release/current-contract.json'));
const nativeRole=currentContract.routines.find(row=>row.schema==='public'&&row.identity.startsWith('weekly_source_message_dispatch_submission_start_atomic_v1('));
assert.equal(nativeRole.owner,'postgres');assert.equal(nativeRole.security_definer,true);
assert(!nativeRole.acl.some(row=>['PUBLIC','public','anon','authenticated'].includes(row.grantee)));
const repairPath='docs/healthroster-build/proposals/v6-contact-assembly-review-20261016/forward/01_complete_current_closed_command_start.sqlinc';
const repair=fs.readFileSync(path.join(money,repairPath));assert.equal(sha(repair),'524fd763e1f0822b64cf3f471b99b22461083516df3a3be2daa07085bf037628');
assert.equal(lf(repair).trim(),closed.sql);assert.equal(repair.length,414);
const repairCommit='230c3bd58049406ea6ba87662160823dc43e2e52';assert.deepEqual(blob(repairCommit,repairPath),repair);
const repairReceiptPath='docs/healthroster-build/proposals/v6-contact-assembly-review-20261016/BLOCKER_REPAIR_INPUT.json';
assert.equal(sha(blob(repairCommit,repairReceiptPath)),'45e0f555d76f884b34fc2ba4b0cf20e7d1a4ee170db3fb8a0ade75ef027c2197');
const result={version:'CONTACT_DISPATCH_AUTHORITY_INDEPENDENT_CROSSREVIEW_V1',status:'SOURCE_ONLY_P1_CONFIRMED_COMPLETE_NATIVE_CLOSED_SUCCESSOR_APPROVED',
 frozenProducer:sourceCommit,historicalProposal:'2d74b1fdced14d4a60882720d7e590ad83f8dd5a',rootCheckpoint:rootCommit,
 oldOpenStart:{...((({sql,...row})=>row)(old)),providerAttemptAllocator:true,commonPermitAdmission:false,perTargetAttempt:false,serviceExecutableByFinal99:true},
 actualNativeClosedStart:{...((({sql,...row})=>row)(closed)),nativeChain,roleContract:nativeRole},
 selectedProtectedOwnerIntersections:intersections,providerAttemptWriters:writers.map(({sql,...row})=>row),
 prerequisiteFunctions:prerequisites.map(({sql,...row})=>({...row,readOnly:true,legacyStartCalls:false})),
 activeOwners:[activeStart,activeResult,wrapper,admission].map(({sql,...row})=>row),
 actualAdmissionBeforeTargetAttempt:true,actualBoundPermitReturned:true,actualOriginalAttemptReplayRetained:true,
 actualResultUsesSourceResultBeforeAttemptCompletion:true,actualResultAggregatesSiblingTargets:true,runtimeUsesOnlyPerTargetStartResult:true,
 completeRepair:{sourceCommit:repairCommit,path:repairPath,savedSha256:sha(repair),bytes:repair.length,completeStatementSha256:closed.sha256,
  repairReceiptPath,repairReceiptSha256:sha(blob(repairCommit,repairReceiptPath)),frozenRepairEqualsSaved:true,
  completeStatementEqualsActualNativeClosedOwner:true,authority:'Exact committed native closed function from rootCheckpoint; saved Source repair has identical complete body.',
  noNewGrantOrAbi:true,proposedOrder:'Complete frozen base through51, complete closed owner successor, guard650, exact final99; root reconciles finalPhysical to closed successor.'},
 neededNative:['Legacy command START and RESULT both still raise exact55000 through actual service role and allocate no attempts/permits.',
  'Actual per-target first-ever/mixed-scope PAUSE/START, QT PUSH/EMAIL behavior, permit/content/member/target/currentness validation, unchanged original-attempt replay.',
  'Actual target ACCEPTED/AMBIGUOUS/rejected/transient result and sibling aggregation, permit consumption/native memberships once, conflicting replay refusal.',
  'Final owner/role/PostgREST/registry closure after exact full Contact assembly; no fake provider receipt.'],
 native:'NOT_RUN',databaseConnections:0,providerCalls:0,rootEdits:false,bankingEdits:false};
fs.writeFileSync(path.join(here,'EVIDENCE.json'),JSON.stringify(result,null,2)+'\n');
const files=['REVIEW.md','audit.mjs','EVIDENCE.json'].map(file=>{const bytes=fs.readFileSync(path.join(here,file));assert(!bytes.includes(Buffer.from('\r\n')));return {path:file,sha256:sha(bytes),bytes:bytes.length};});
fs.writeFileSync(path.join(here,'SOURCE_MANIFEST.json'),JSON.stringify({version:'CONTACT_DISPATCH_AUTHORITY_CROSSREVIEW_SOURCE_PINS_V1',
 status:'P1_CONFIRMED_EXACT_NATIVE_CLOSED_SUCCESSOR_APPROVED_SOURCE_ONLY',frozenProducer:sourceCommit,repairCommit,rootCheckpoint:rootCommit,files,excludesSelf:true,
 native:'NOT_RUN',databaseConnections:0,providerCalls:0,rootEdits:false,bankingEdits:false,priorReviewsAndProposals:'PRESERVED'},null,2)+'\n');
console.log(JSON.stringify({selectedFunctions:all.length,providerAttemptWriters:writers.length,protectedOwnerShadows:intersections.filter(row=>row.selectedDefinitions.length).map(row=>row.signature),repairSha256:sha(repair),repairMatchesCompleteNative:true,native:'NOT_RUN'}));
