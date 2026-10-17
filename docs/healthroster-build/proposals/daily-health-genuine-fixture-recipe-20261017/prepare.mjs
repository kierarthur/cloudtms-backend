// Manifest/source recipe only. No mutation, network client or execution adapter.
import fs from'node:fs';import path from'node:path';import{fileURLToPath}from'node:url';import{execFileSync}from'node:child_process';import{createHash}from'node:crypto';import assert from'node:assert/strict';
const here=path.dirname(fileURLToPath(import.meta.url)),root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',commit='fc302f15103102a87903bada1b61ad68dfd0cc05',sha=b=>createHash('sha256').update(b).digest('hex');
assert(fs.realpathSync.native(here).replaceAll('\\','/').startsWith('D:/Development/GitHub/.codex-worktrees/healthroster-checklist-native-14102026/'));assert(!fs.existsSync(here+'/MANIFEST.json'));
const files=['broker/src/index.js','broker/src/daily-rate-correction.js','broker/src/daily-canonical-authority.js','broker/src/daily-schedule-authority.js','supabase/repeatable/16102026_0858_nhsp_v6_daily_native_authority/01_context.sqlinc','supabase/repeatable/16102026_0858_nhsp_v6_daily_native_authority/02_admission.sqlinc','supabase/repeatable/17102026_0248_nhsp_v6_daily_health_support/01_complete_helpers.sqlinc','supabase/repeatable/22092026_1226_client_initial_settings_baseline_v1.sql','supabase/release/current-contract.json','docs/bpay-next/final-definer-closures.json'];
const pins=files.map(file=>{const b=execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,'show',commit+':'+file],{maxBuffer:16e6});assert(b.equals(fs.readFileSync(root+'/'+file)),file);return{file,bytes:b.length,sha256:sha(b)};});
const contract=JSON.parse(fs.readFileSync(root+'/supabase/release/current-contract.json'));
const relations=['clients','client_settings','candidates','rates_client_defaults','rates_candidate_overrides','timesheets','timesheets_financials'];
const metadata={source:'EXACT_COMMITTED_CONTRACT_NOT_FRESH_NATIVE',contractSha256:pins.find(p=>p.file==='supabase/release/current-contract.json').sha256,
 relations:relations.map(name=>{const r=contract.relations.find(r=>r.schema==='public'&&r.name===name);assert(r);return r;}),
 triggers:contract.triggers.filter(t=>t.schema==='public'&&relations.includes(t.table)),
 additionalCurrentSourceTriggers:[{sourceFile:files[4],names:['daily_rate_authority_client_lock_v1','daily_rate_authority_candidate_lock_v1'],nativeInstalled:'NOT_PROVED'}]};
const registry=JSON.parse(fs.readFileSync(root+'/docs/bpay-next/final-definer-closures.json'));
const signatures=['public.client_create_with_settings_v1(uuid,jsonb,uuid,jsonb,timestamp with time zone)','public.timesheet_authorise_generic_atomic(uuid,uuid,uuid,timestamp with time zone,text)'];
const ownerMetadata=signatures.map(signature=>{const matches=registry.chains.filter(r=>r.signature===signature);assert.equal(matches.length,1);return{signature,finalRoot:matches[0].finalRoot,finalPhysical:matches[0].finalPhysical,bodyInspected:false};});
const client='f717d043-0000-4000-8000-000000000010',candidate='f717d043-0000-4000-8000-000000000030';
const flags={effective_from:'2026-10-01',timezone_id:'Europe/London',default_submission_mode:'MANUAL',no_timesheet_required:true,
 requires_hr:false,hr_validation_required:false,autoprocess_hr:false,is_nhsp:false,self_bill_no_invoices_sent:true,
 daily_calc_of_invoices:false,auto_invoice_default:false,hr_attach_to_invoice:false,ts_attach_to_invoice:false,
 ts_reference_required:false,pay_reference_required:false,invoice_reference_required:false,
 reference_number_required_to_issue_invoice:false,opt_in_email:false,opt_in_sms:false,opt_in_whatsapp:false,
 healthroster_import_auto_authorise:false,nhsp_import_auto_authorise:false};
const clientBody={id:client,name:'DAILY_HEALTH_BASIS_20261017_CLIENT',vat_chargeable:false,payment_terms_days:30,mileage_charge_rate:0,
 primary_invoice_email:null,ts_queries_email:null,contact_email:null,client_settings:flags};
const candidateBody={id:candidate,key_norm:'daily-health-basis-20261017-candidate',display_name:'DAILY_HEALTH_BASIS_20261017_CANDIDATE',active:true,
 roles:['RN'],pay_method:'PAYE',umbrella_id:null,email:null,account_holder:null,bank_name:null,sort_code:null,account_number:null,
 remittance_receive_enabled:false,remittance_overrides_enabled:false};
const rates={client_id:client,role:'RN',band:'Band 6',date_from:'2026-10-01',date_to:'2026-10-31'};
for(const k of['day','night','sat','sun','bh'])Object.assign(rates,{['charge_'+k]:30,['paye_'+k]:20,['umb_'+k]:25});
const createDaily={candidate_id:candidate,client_id:client,role:'RN',band:'Band 6',hospital:clientBody.name,ward:'DAILY_HEALTH_BASIS_20261017',
 shift_label:'DAILY_HEALTH_BASIS_20261017_SHIFT',schedule_json:{date:'2026-10-15',start:'08:00',end:'14:00',break_minutes:0}};
const recipe={version:'DAILY_HEALTH_GENUINE_FIXTURE_DRAFT_V1',status:'MANIFEST_ONLY_NOT_CREATED',sourceCommit:commit,root,
 sourcePins:pins,ownerMetadata,authority:'Existing SEND5 D9 TEST setup; root must append exact staged manifest and review before mutation',
 plannedClientId:client,plannedCandidateId:candidate,preExistingActorId:null,nativeGeneratedIds:{client_settings_id:null,rate_default_id:null,timesheet_id:null,financial_id:null,booking_id:null},
 freshVacancyReceipt:null,actualInstalledSourceReceipt:null,canonicalSetupBaselineReceipt:null,workerPreparationReceipt:null,
 stages:[
  {id:'S0',action:'READ_ONLY_BIND_AND_MANIFEST',requires:['fresh agency_test identity/customer-null/LEGACY:1/release-idle','current real active Admin and WP-PP actor; do not revoke a real user for a race','both IDs, unique name/key, candidate/client aliases and derived booking scope vacant','native owner/schema/ACL/trigger availability; source hashes alone are insufficient','root exact TEST_SETUP_MANIFEST append committed'],mutations:0},
  {id:'S1',method:'POST',path:'/api/clients',body:clientBody,owner:'createClientWithSettingsAtomic -> public.client_create_with_settings_v1',after:'one exact Client and genuine initial effective settings row/native authority; bind native settings ID before next stage'},
  {id:'S2',method:'POST',path:'/api/candidates',body:candidateBody,owner:'handleCreateCandidate; native candidate INSERT and scoped identity assignment',after:'one native Candidate, real minted reference, no app user/login/invitation/bank account; unique key matches zero existing Timesheets'},
  {id:'S3',method:'POST',path:'/api/rates/client-defaults',body:rates,owner:'handleUpsertClientRate; actual unified native dated selector store',after:'one enabled Client default covering work date, zero candidate overrides; bind actual rate ID; no overlapping or existing window is truncated'},
  {id:'S4',method:'POST',path:'/api/timesheets/create-daily-manual',body:createDaily,owner:'handleTimesheetCreateManualDaily -> saveAndRecalculateCanonicalDaily -> enqueue_ts_financials_priority/runTsfinWorkerOnce',after:'capture native-generated Timesheet/booking/current TSFIN IDs and exact readback before next mutation; original source row must remain DAILY/HOURS/no Contract/version1/current/not adjustment'},
  {id:'S5',method:'POST',path:'/api/timesheets/{ACTUAL_NATIVE_TIMESHEET_ID}/authorise',body:null,owner:'handleTimesheetAuthoriseGeneric -> existing public.timesheet_authorise_generic_atomic',requires:'bind latest actual physical Timesheet ID and current row signature via genuine native read; append exact body/ID in manifest BEFORE this request',after:'native current SELF_REPORTED TSFIN and physical Timesheet both authorised; same actual version/candidate/client; frozen settings authority present; retain all ordinary native admission/hooks'},
  {id:'S6',action:'READ_NATIVE_CONTEXT_AND_REAL_WORKER_PREPARATION',owner:'public.timesheet_daily_rate_correction_context_v1 plus actual tsfin_resolve_rates_batch and exact dispatchDailyRateCorrectionRequest calculator',requires:'actual mounted/installed owner availability or independently reviewed rollback closure; do not assume source-only definitions exist in TEST',after:'actual context/native rate rows and actual Worker-produced trusted PREVIEW args, no substituted snapshot, no fabricated public Preview success'},
  {id:'S7',action:'ROOT_OWNED_UNLOCKED_BASIS_AND_INCOMPLETE_REVIEW_TRIAL',requires:'reviewed closed input adapter, full canonical setup baseline, genuine context/snapshot from S6, shared turn/real marker/original complete observations and WeakMap restoration',after:'p_lock=false Health basis may be tested; partial review must fail genuine deferred commit guard; every temporary metadata row rolls back; no public composer or Bank result claimed'}
 ],
 holds:[
  'Native generated IDs/signatures/actor/source availability/vacancy are unbound; no executable mutation input is produced',
  'Existing create source stores job_title_norm lowercase; real Worker normalizes financial role uppercase; current Health0858 verbatim comparison is a source mismatch candidate retained for separate forward review; actual native readback NOT_PROVED',
  'Independent Worker/PostgREST requests cannot see uncommitted rollback setup. Root must use D9 committed exact setup baseline, or separately review a genuinely same-session producer transport; nested rollback is insufficient',
  'Public Daily composers and genuine Bank joins remain unprovided; a capture-only real Worker boundary must refuse absent Preview rather than return nominal success',
  'No rate change is needed for initial genuine basis/guard proof; this does not prove monetary difference or Accept. Later selector changes/races need exact separate setup and real commit visibility',
  'Locked basis remains dependent on existing Banking F17 NEXT readiness. No control flip or new owner permission question',
  'Real API role runtime, dated-selector/actor races and RR/serializable trials remain separate; do not revoke real operator permissions'
 ],businessRowsCreated:0,native:'NOT_RUN',snapshotInvented:false,bankingBodiesInspected:false};
fs.writeFileSync(here+'/RECIPE.json',JSON.stringify(recipe,null,2)+'\n');fs.writeFileSync(here+'/SCHEMA_METADATA.json',JSON.stringify(metadata,null,2)+'\n');
console.log(JSON.stringify({stages:recipe.stages.length,plannedFixedIds:2,nativeGeneratedIdsUnbound:5,native:'NOT_RUN',created:0}));
