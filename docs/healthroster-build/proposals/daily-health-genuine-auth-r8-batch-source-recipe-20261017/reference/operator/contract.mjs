// Finite fixture protocol, not a product endpoint or a financial calculator.
import fs from 'node:fs';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
import {STAGE as mappingStage} from './reference/mapping/stage.mjs';
export const ROOT='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
export const FRONT='https://testmode.arthur-rai.co.uk';
export const BACK='https://test-cloudtms-backend.kier-88a.workers.dev';
export const PRIVATE_STATE=ROOT+'/.codex-tmp/test-auth-refresh-20261017-2d7353fef2593967050335a847b9fa0d/storage-state.json';
export const PW='file:///D:/Development/GitHub/TEST-Frontend/node_modules/@playwright/test/index.mjs';
export const CLIENT='f717d043-0000-4000-8000-000000000010';
export const CANDIDATE='f717d043-0000-4000-8000-000000000030';
export const BOOKING='bk_9364434070884e36';
export const MAPPING=mappingStage.body.id;
export const STAGES=['S0','S1','S1a','S2','S3','S4'];
export const check=(v,c)=>{if(!v)throw Error(c);};
export const sha=v=>crypto.createHash('sha256').update(v).digest('hex');
export const exact=(v,keys)=>v&&typeof v==='object'&&!Array.isArray(v)&&Object.keys(v).sort().join(',')===[...keys].sort().join(',');
export const uuid=v=>typeof v==='string'&&/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(v);
const recipe=JSON.parse(fs.readFileSync(new URL('./reference/daily-health-genuine-fixture-recipe-20261017/RECIPE.json',import.meta.url)));
export const route={S1:'/api/clients',S1a:mappingStage.path,S2:'/api/candidates',S3:'/api/rates/client-defaults',S4:'/api/timesheets/create-daily-manual'};
export function request(stage){check(route[stage],'FIXTURE_STAGE_REQUIRED');return structuredClone(stage==='S1a'?mappingStage.body:recipe.stages.find(s=>s.id===stage).body);}
export const requestSha=stage=>sha(JSON.stringify(request(stage)));
const includes=(row,expected)=>row&&Object.entries(expected).every(([k,v])=>JSON.stringify(row[k])===JSON.stringify(v));
export function qualifyResponse(stage,status,body){
 check(status===200,'FIXTURE_RESPONSE_RECONCILIATION_REQUIRED');
 const input=request(stage);
 if(stage==='S1'){
  check(exact(body,['ok','replay','client','client_settings','policy'])&&body.ok===true&&body.replay===false,'FIXTURE_S1_RECEIPT_REFUSED');
  check(includes(body.client,{id:CLIENT,name:input.name,vat_chargeable:false,payment_terms_days:30,mileage_charge_rate:0,primary_invoice_email:null,ts_queries_email:null,contact_email:null}),'FIXTURE_S1_CLIENT_REFUSED');
  // The genuine initial-settings owner fixes 1900, although the approved historical request supplied October.
  check(uuid(body.client_settings?.id)&&includes(body.client_settings,{...input.client_settings,client_id:CLIENT,effective_from:'1900-01-01'}),'FIXTURE_S1_BASELINE_REFUSED');
  check(exact(body.policy,['healthroster','nhsp'])&&body.policy.healthroster&&typeof body.policy.healthroster==='object'&&!Array.isArray(body.policy.healthroster)
   &&body.policy.nhsp&&typeof body.policy.nhsp==='object'&&!Array.isArray(body.policy.nhsp),'FIXTURE_S1_NATIVE_POLICY_REQUIRED');
  return {clientId:CLIENT,settingsId:body.client_settings.id,baseline:'1900-01-01'};
 }
 if(stage==='S1a'){
  check(exact(body,['hospital'])&&includes(body.hospital,{...input,client_id:CLIENT}),'FIXTURE_S1A_MAPPING_REFUSED');
  return {mappingId:MAPPING};
 }
 if(stage==='S2'){
  check(exact(body,['candidate','tsfin'])&&includes(body.candidate,input)&&uuid(body.candidate?.id)
   &&typeof body.candidate.tms_ref==='string'&&body.candidate.tms_ref.length>0,'FIXTURE_S2_CANDIDATE_REFUSED');
  const fields=['enqueued','ran_now','picked','ok','fail','auto_assigned_timesheets'];
  check(exact(body.tsfin,fields)&&fields.every(k=>body.tsfin[k]===0),'FIXTURE_S2_NONEMPTY_DRAIN_REFUSED');
  return {candidateId:CANDIDATE,emptyDrain:true};
 }
 if(stage==='S3'){
  check(exact(body,['rate'])&&uuid(body.rate?.id)&&includes(body.rate,input)&&body.rate.disabled_at_utc===null,'FIXTURE_S3_RATE_REFUSED');
  return {rateId:body.rate.id};
 }
 check(stage==='S4'&&exact(body,['timesheet_id','current_timesheet_id','created_unprocessed'])
  &&uuid(body.timesheet_id)&&body.current_timesheet_id===body.timesheet_id&&body.created_unprocessed===true,'FIXTURE_S4_TIMESHEET_REFUSED');
 return {timesheetId:body.timesheet_id};
}
export function nextStage(qualified){
 check(Array.isArray(qualified)&&qualified.every((s,i)=>s===STAGES[i]),'FIXTURE_QUALIFIED_PREFIX_REQUIRED');
 return STAGES[qualified.length]??null;
}
export function entryText(stage,{sourceCommit,beforeSqlSha256,verificationSqlSha256,beforeEnvelopeSha256,effectAdmissionSha256}){
 check(STAGES.includes(stage),'FIXTURE_STAGE_REQUIRED');
 for(const s of [sourceCommit])check(/^[0-9a-f]{40}$/.test(s),'FIXTURE_SOURCE_COMMIT_REQUIRED');
 for(const s of [beforeSqlSha256,verificationSqlSha256,beforeEnvelopeSha256,effectAdmissionSha256])check(/^[0-9a-f]{64}$/.test(s),'FIXTURE_NATIVE_PIN_REQUIRED');
 const body=stage==='S0'?null:request(stage);
 return `\n\n### DAILY-GENUINE-${stage}-20261017 — one bounded staged operator call\n\n`
  +`SEND_5 D9; agency_test/cloudtms_test_clone only. Operator source ${sourceCommit}. Client ${CLIENT}; Candidate ${CANDIDATE}; booking ${BOOKING}. No role changes, direct seeds, authorisation, email/provider/payment/Banking action, cleanup or retry. S0 is read-only. Stages S1–S4 each permit exactly one genuine normalTEST POST after this exact entry is committed.\n\n`
  +`Fresh native main SQL SHA256 ${beforeSqlSha256}; fresh verification SQL SHA256 ${verificationSqlSha256}; actual before envelope SHA256 ${beforeEnvelopeSha256}; separately reviewed installed Worker/native/schema/ACL/trigger/default/effect admission SHA256 ${effectAdmissionSha256}. The actual original shared-turn A remains owned through the readback; fresh current active Admin/WP-PP, TEST/customer-null/LEGACY:1/no APPLYING and vacancy are mandatory. Metadata or an old source pin alone is not effect admission.\n\n`
  +`Exact request: ${stage==='S0'?'NONE':'POST '+BACK+route[stage]}\n\n\`\`\`json\n${JSON.stringify(body,null,2)}\n\`\`\`\n\n`
  +`Expected scope: S1 one genuine Client plus native initial settings effective1900-01-01, current policy resolution/audit; S1a one exact native mapping ${MAPPING} through the existing Hospital POST, one lower-case alias and ward_hint null, zero global current/history alias Timesheets and wakeup targets; S2 one non-login/non-bank Candidate and zero auto-assigned/financial targets; S3 one genuine dated native rate with empty prior rate/override/Client-TSFIN scope; S4 one native DAILY/HOURS/MANUAL/contract-null/current-version1 Timesheet and genuine unprocessed SELF_REPORTED TSFIN through the exact-ID canonical owner, with native outbox completion. S4 requires the genuinely qualified S1a association. No invented financial output.\n\n`
  +`Before and fresh after native aggregates contain the entire reserved Client/settings/Candidate/join/rate/override/booking/current+historical Timesheet/TSFIN/outbox scope, exact native metadata and fingerprints. Preserve every prior qualified fixture row; only the stage's approved native changes may occur. A 200 response is insufficient without full readback. Any timeout, malformed response, refusal or non-200 permanently retains the attempt as unresolved. S4 HTTP500 may follow committed INSERT/recalculation and failed DELETE: reconcile booking/current+history/financial/outbox before any separately reviewed retry. This operator never retries, repairs, deletes, authorises or advances an uncertain attempt. Full native/API/financial proof remains NOT_RUN before genuine observation.\n`;
}
