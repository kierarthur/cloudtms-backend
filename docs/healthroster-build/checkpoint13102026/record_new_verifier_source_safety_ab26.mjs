import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const root='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-v2-backend-13102026';
const commit='ab26a5ff22200be037d215b1100c969d6c0bae73';
const out=path.dirname(fileURLToPath(import.meta.url));
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const git=args=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{cwd:root,maxBuffer:32000000});
const canonical=b=>b.toString('utf8').replace(/\r\n/g,'\n');
const files=[
 'supabase/verification/13102026_2111_healthroster_report_native_fin_worker_v1.sql',
 'supabase/verification/13102026_2206_weekly_source_automatic_native_delta_v1.sql',
 'supabase/repeatable/13102026_2110_healthroster_report_native_fin_worker/02_pay_authority.sqlinc',
 'supabase/repeatable/13102026_2110_healthroster_report_native_fin_worker/03_pay_witness.sqlinc',
 'supabase/repeatable/13102026_2205_weekly_source_automatic_native_delta/02_staged_publication_context.sqlinc',
 'supabase/repeatable/13102026_2205_weekly_source_automatic_native_delta/08_complete_automatic_reconcile_delta_forward.sqlinc',
 'supabase/repeatable/13102026_1438_weekly_source_protection_queries/25_bulk_stage_proof.sqlinc',
 'supabase/repeatable/13102026_1438_weekly_source_protection_queries/27_automatic_reconcile.sqlinc'
];
const pins=files.map(file=>{
 const bytes=git(['show',commit+':'+file]),saved=fs.readFileSync(path.join(root,file));
 if(canonical(bytes)!==canonical(saved))throw Error('REVIEW_SOURCE_DRIFT');
 return {file,gitBlobBytes:bytes.length,gitBlobSha256:sha(bytes),canonicalSha256:sha(canonical(bytes)),savedBytes:saved.length,savedSha256:sha(saved),completeBodyRead:true};
});
const result={status:'FOCUSED_COMPLETE_SOURCE_EFFECT_REVIEW_NOT_EXECUTED',sourceCommit:commit,
 scope:'Two new verifier roots only. Neither historical168 review/exclusions nor any frozen/native acceptance receipt is rewritten. Complete post-Banking composed-source review remains mandatory.',
 reviewedBodies:pins,
 verifiers:[
 {file:files[0],sourceDecision:'INCLUDE_DATA_FREE_DECLARATION_AND_NULL_NEGATIVES_AT_PIN',
  effects:['pg_proc ownership/defaults/proconfig/security kind and effective browser/service/PUBLIC EXECUTE inspection','No role impersonation, DDL, DML, row/advisory lock, queue claim/drain, payment, provider, email or network call'],
  applicationCalls:[{name:'private.weekly_source_self_bill_report_pay_authority_v1',arguments:'NULL',path:'SELECT binding WHERE id=NULL returns no rows; immediate NULL before any nested inventory/financial/NEXT/submission reader'},
   {name:'private.weekly_source_self_bill_report_pay_witness_v1',arguments:'NULL',path:'Calls the same authority; immediate NULL before compact invoice witness construction'}],
  metadataOnly:['private.weekly_source_report_projection_receipt_assert_v1','private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean,text)'],
  held:'Report A/B admission/rendering/pay isolation, positive native financial/capture/NEXT/transport proof and actual complete owner first use are not performed'},
 {file:files[1],sourceDecision:'INCLUDE_DATA_FREE_HASH_ACL_AND_EARLY_REFUSALS_AT_PIN',
  effects:['Exact eleven-helper body hash/ownership/security/STABLE/return-type/default/proconfig/effective ACL checks and four retained owner body hashes','NULL selectors raise22023 before table lookup or capability/component/financial branches','Missing sentinel is first proved absent, then SELECT STRICT raises no_data_found mapped to exact55000 before downstream native owners','Missing-dependency calls run only when their actual to_regprocedure absence is proved; wrappers raise exact55000 before their dynamic SELECTs'],
  applicationCalls:['private.weekly_source_staged_publication_context_v1(NULL,NULL)','private.weekly_source_automatic_reconcile_delta_assert_v1(NULL,NULL)','same two helpers with exact vacant sentinel13102026-2206-4000-8000-000000000001','private.weekly_source_automatic_reconcile_capability_assert_v1(NULL) only if producer-ready signature is absent','private.weekly_source_bulk_source_cas_assert_v1(NULL,empty uuid[]) only if either actual dependency is absent'],
  notInvoked:['Eleven helper/retained-owner hash list entries other than the exact early-refusal calls','public.weekly_source_automatic_reconcile_atomic_v1','financial producer/component/delta positive branches','Local/C1/NEXT completion and any receipt/queue writer'],
  held:'A present dependency is not readiness. Conditional absence arms legitimately do not run after actual dependencies are installed; their positive/false-readiness qualification needs separate complete native gates, never a made-up READY helper. The earlier79-statement lab subset receipt remains separate and does not certify the full release'}
 ],
 harnessRequirements:['One server-owned or approved full-fallback outer transaction; normalize only exact reviewed BEGIN/ROLLBACK/ON_ERROR_STOP envelopes and preserve body assertions','Existing isolated verifier savepoint/client-variable protocol, mandatory whole-run isolation review and fresh post-rollback comparison','Before any actual execution: completed Banking first-install and lock release, exact current TEST/source integration, SEND7 turn and no active protected release','Re-review any changed controlling callee, schema, trigger or release source; no generic reuse across changed commits'],
 actualDatabaseCalls:0,actualNetworkBusinessCalls:0,nativeAcceptanceClaimed:false,finalPartBCritic:false,
 originalReview:{reviewed:168,eligible:152,excluded:16,unchanged:true},sourceReviewDoesNotAuthorizeRelease:true};
const target=path.join(out,'NEW_VERIFIER_SOURCE_SAFETY_REVIEW_AB26.json');
if(fs.existsSync(target))throw Error('EXISTING_REVIEW_REFUSED');
fs.writeFileSync(target,JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify({status:result.status,sourceCommit:commit,roots:2,completeBodies:pins.length,sha256:sha(fs.readFileSync(target)),actualDatabaseCalls:0}));
