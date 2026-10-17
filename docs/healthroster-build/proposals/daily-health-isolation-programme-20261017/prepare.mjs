// Capture exact committed Health input bytes. No native calls or root writes.
import fs from 'node:fs';import path from 'node:path';import{createHash}from'node:crypto';import{execFileSync}from'node:child_process';import{fileURLToPath}from'node:url';import assert from'node:assert/strict';
import{ROOT,definition,MODES,buildIsolationProgram,renderPreQueryReviewScript}from'./build-program.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),commit='b4cb19f04be68f67b8aad81692c26e96f7b6f0ed',sha=b=>createHash('sha256').update(b).digest('hex');
const git=file=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',ROOT,'show',commit+':'+file],{maxBuffer:16e6});
assert(fs.realpathSync.native(here).replaceAll('\\','/').startsWith('D:/Development/GitHub/.codex-worktrees/healthroster-checklist-native-14102026/'));
assert(!fs.existsSync(here+'/MANIFEST.json'),'Frozen packet must not be recaptured');
const selection=[
 ['supabase/repeatable/includes/nhsp_v6_health_composition_wrappers.sqlinc','private.timesheet_contract_move_reason_v1'],
 ['supabase/repeatable/16102026_0858_nhsp_v6_daily_native_authority/03_requests.sqlinc','private.daily_rate_correction_request_assert_v1'],
 ['supabase/repeatable/17102026_0248_nhsp_v6_daily_health_support/01_complete_helpers.sqlinc','private.daily_rate_correction_health_actor_require_v1'],
 ['supabase/repeatable/17102026_0248_nhsp_v6_daily_health_support/01_complete_helpers.sqlinc','private.daily_rate_correction_health_basis_v1']
];
const definitions=selection.map(([file,name])=>{const b=git(file);assert(b.equals(fs.readFileSync(ROOT+'/'+file)),file);const body=definition(b,name),saved='inputs/current/'+file;fs.mkdirSync(path.dirname(here+'/'+saved),{recursive:true});fs.writeFileSync(here+'/'+saved,b);return{file,name,fileSha256:sha(b),completeParent:saved,sha256:sha(body),bytes:body.length};});
const guards=['scripts/healthroster-install1-full-rehearsal.mjs','scripts/healthroster-live-test-restoration.mjs','scripts/healthroster-full-rehearsal-protocol.mjs','scripts/healthroster-rehearsal-turn.mjs','scripts/healthroster-turn-control.mjs','supabase/release/export_contract.sql'];
const preserved=guards.map(file=>{const b=git(file);assert(b.equals(fs.readFileSync(ROOT+'/'+file)),file);return{file,sha256:sha(b),bytes:b.length};});
const pins={version:'DAILY_HEALTH_ISOLATION_EXACT_SOURCE_V1',root:ROOT,sourceCommit:commit,runtimeSourceCommit:'84738fe4057d9d693ccd723eb1835106f1091f57',definitions,preserved,
 snapshotRelations:['private.candidate_daily_batch_receipts','public.tms_users','public.ts_financials_outbox'],
 directBusinessReads:['public.tms_users'],businessFixtureRows:0,native:'NOT_RUN',executionAdapter:'UNPROVIDED',
 policy:'Existing locked basis RC-only guard; no new restriction, actor or financial permission',bankingBodies:[]};
fs.writeFileSync(here+'/SOURCE_PINS.json',JSON.stringify(pins,null,2)+'\n');
const examples=MODES.flatMap(isolation=>[{name:isolation,kind:'BODY',...buildIsolationProgram({root:ROOT,isolation})},{name:isolation+' REVIEW SCRIPT',kind:'REVIEW_SCRIPT',...renderPreQueryReviewScript({root:ROOT,isolation})}]);
fs.writeFileSync(here+'/PROGRAMMES.json',JSON.stringify({version:'DAILY_HEALTH_ISOLATION_SOURCE_EXAMPLES_V1',syntheticFinancialFacts:false,dbConnections:0,examples},null,2)+'\n');
console.log(JSON.stringify({definitions:4,completeParents:3,examples:examples.length,bytes:examples.map(x=>({name:x.name,bytes:x.bytes,sha256:x.sha256})),native:'NOT_RUN'}));
