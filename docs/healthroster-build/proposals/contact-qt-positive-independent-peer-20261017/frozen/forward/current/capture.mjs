// Source capture only. This module opens no connection and invokes no native trial.
import fs from 'node:fs';import path from 'node:path';import {fileURLToPath,pathToFileURL}from'node:url';
import {execFileSync}from'node:child_process';import {createHash}from'node:crypto';import assert from'node:assert/strict';
const here=path.dirname(fileURLToPath(import.meta.url)),root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',commit='feb36c0c13c87b57b7566172e4bfaa1254117794';
const sha=b=>createHash('sha256').update(b).digest('hex'),git=f=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,'show',commit+':'+f],{maxBuffer:24e6});
assert(fs.realpathSync.native(here).replaceAll('\\','/').startsWith('D:/Development/GitHub/.codex-worktrees/healthroster-checklist-native-14102026/'));
assert(!fs.existsSync(here+'/MANIFEST.json'));
const pins=[],capture=(file,copy)=>{const b=git(file);assert(b.equals(fs.readFileSync(root+'/'+file)),file);fs.mkdirSync(path.dirname(here+'/'+copy),{recursive:true});fs.writeFileSync(here+'/'+copy,b);pins.push({file,copy,bytes:b.length,sha256:sha(b)});return b;};
const files=[
 ['supabase/migrations/16102026_1205_weekly_contact_20_automatic_followup_tranche.sql','reference/contact20.sql'],
 ['supabase/migrations/16102026_1205_weekly_contact_30_submission_cohort_identity.sql','reference/contact30.sql'],
 ['supabase/migrations/15102026_0800_candidate_push_timing_authority.sql','reference/qt-schema.sql'],
 ['supabase/repeatable/15102026_0800_candidate_push_timing_authority_v1.sql','reference/qt-authority.sql'],
 ['tests/weekly-source/push-timing-admin-native.sql','reference/original-qt-trial.sql'],
 ['docs/healthroster-build/v6-owner-delivery-20261016/CONTACT_SIX_LOCK_NATIVE_20261017.json','reference/original-six-case-receipt.json']
];for(const[file,copy]of files)capture(file,copy);
const {scanManagedSql}=await import(pathToFileURL(root+'/scripts/cloudtms-managed-release-sql.mjs').href);
const {routineDefinitions}=await import(pathToFileURL(root+'/scripts/bpay-next-final-definers.mjs').href);
const helpers=[['private._weekly_source_settings_require_admin_v1(uuid)','supabase/repeatable/15092026_1534_weekly_source_settings_admin_v1.sql'],
 ['private.weekly_source_sha256_text_v1(text,text)','supabase/repeatable/15092026_1534_00_weekly_source_private_classifiers_v1.sql'],
 ['private.weekly_source_sha256_jsonb_v1(text,jsonb)','supabase/repeatable/15092026_1534_00_weekly_source_private_classifiers_v1.sql']];
const contract=JSON.parse(git('supabase/release/current-contract.json'));
const selected=[];for(const[signature,file]of helpers){const b=git(file);assert(b.equals(fs.readFileSync(root+'/'+file)));const s=b.toString(),scan=scanManagedSql(s);const choices=scan.statements.filter(x=>routineDefinitions(s.slice(x.start,x.end),file).includes(signature));assert.equal(choices.length,1);const body=s.slice(choices[0].start,choices[0].end),copy='reference/helpers/'+signature.split('.')[1].split('(')[0]+'.sqlinc';fs.mkdirSync(path.dirname(here+'/'+copy),{recursive:true});fs.writeFileSync(here+'/'+copy,body);selected.push({signature,file,wholeSourceSha256:sha(b),copy,sha256:sha(body),bytes:Buffer.byteLength(body),nativeExpected:contract.routines.find(r=>r.schema===signature.split('.')[0]&&r.identity.startsWith(signature.split('.')[1].split('(')[0]+'('))});pins.push({file,bytes:b.length,sha256:sha(b)});}
const actor='docs/healthroster-build/proposals/daily-health-actor-native-proof-20261017/';
for(const name of ['build-program.mjs','SOURCE_PINS.json','NATIVE_DEPENDENCY_DIAG_R1.json'])capture(actor+name,'reference/actor/'+name);
for(const name of ['fixed-metadata-reader.mjs','control.mjs','timing.mjs'])capture(actor+name,'reference/adapter/'+name);
const actorPins=JSON.parse(git(actor+'SOURCE_PINS.json'));
const rootInputs=pins.concat(actorPins.rootInputs).concat([
 'scripts/cloudtms-db-release-lib.mjs','scripts/bpay-next-final-definers.mjs'
 ].map(file=>{const b=git(file);assert(b.equals(fs.readFileSync(root+'/'+file)));return{file,bytes:b.length,sha256:sha(b)};}));
const unique=[...new Map(rootInputs.map(x=>[x.file,x])).values()];
const relationNames=['public.weekly_manager_recipient_generations','public.weekly_message_dispatch_commands','public.weekly_candidate_cohorts','public.weekly_timesheet_submission_requests','public.weekly_candidate_outreach_generations','public.weekly_timesheet_submission_request_memberships','public.tms_users','public.settings_defaults','public.weekly_source_global_settings','public.audit_events','private.candidate_daily_batch_receipts','public.ts_financials_outbox'].sort();
const metadata={relations:contract.relations.filter(r=>relationNames.includes(r.schema+'.'+r.name)),triggers:contract.triggers.filter(t=>relationNames.includes(t.schema+'.'+t.table)),policies:contract.policies.filter(p=>relationNames.includes(p.schema+'.'+p.table))};
fs.writeFileSync(here+'/reference/source-store-metadata.json',JSON.stringify(metadata,null,2)+'\n');
const out={version:'CONTACT_QT_POSITIVE_SOURCE_AUTHORITY_V1',root,rootCommit:commit,rootInputs:unique,
  selectedHelpers:selected,fixtureIDs:actorPins.fixtureIDs,fixtureEmails:actorPins.fixtureEmails,userStore:actorPins.userStore,
  expectedWritersSha256:actorPins.expectedWritersSha256,relationNames,
  actorPreflightSource:actor+'build-program.mjs',nativeTouchDiagnostic:actor+'NATIVE_DEPENDENCY_DIAG_R1.json',
  cases:['CONTACT_CONSTRUCTORS_V1','QT_ADMIN_CONTROLS_V1'],bankBodiesSelected:0,native:'NOT_RUN'};
fs.writeFileSync(here+'/SOURCE_PINS.json',JSON.stringify(out,null,2)+'\n');
console.log(JSON.stringify({rootInputs:unique.length,completeSelectedHelpers:selected.length,snapshotRelations:relationNames.length,native:'NOT_RUN'}));
