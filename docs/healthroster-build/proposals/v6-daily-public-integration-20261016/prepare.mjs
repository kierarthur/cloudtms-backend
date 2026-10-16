// Offline assembly/reverse proof only. Does not write I2 or open a DB.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath,pathToFileURL} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url));
export const root='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-install2-14102026';
const pack='C:/tmp/healthroster-handover/HealthRoster_Builder_Pack_16102026';
export const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const read=(base,f)=>fs.readFileSync(path.join(base,f));
const git=(...a)=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...a],{cwd:root,encoding:'utf8',windowsHide:true}).trim();
const pin=(base,f)=>{const b=read(base,f);return {file:f,bytes:b.length,sha256:sha(b)};};
const dto='broker/src/contract-correction-v3.mjs';
const canonical=await import(pathToFileURL(path.join(root,dto)).href);
const serviceFile=path.join(here,'forward/sql/01_public_services.sqlinc');
let service=fs.readFileSync(serviceFile,'utf8');
const literal=JSON.stringify(canonical.ACTION_VARIANTS).replaceAll("'","''");
if(service.includes('__ACTION_VARIANTS_JSON__')){
 if(service.split('__ACTION_VARIANTS_JSON__').length!==2)throw Error('ACTION_SOURCE_MARKER_COUNT');
 service=service.replace('__ACTION_VARIANTS_JSON__',literal);fs.writeFileSync(serviceFile,service);
}else if(!service.includes("'"+literal+"'::jsonb"))throw Error('PINNED_ACTION_GENERATION_CHANGED');
// Freeze already-mounted complete Health module/calculator/DTO as verification
// inputs. These exact files are VERIFY_EXISTING, not a second overwrite owner.
const modules=['broker/src/daily-rate-correction.js','broker/src/daily-canonical-authority.js',dto];
for(const f of modules){fs.mkdirSync(path.join(here,'existing-exact',path.dirname(f)),{recursive:true});fs.writeFileSync(path.join(here,'existing-exact',f),read(root,f));}
const parentFile='broker/src/index.js',before=read(root,parentFile).toString('utf8');
const changes=[
 {id:'DailyImport',anchor:"import { dispatchWeeklySourceRequest, weeklySourceDeploymentContext } from './weekly-source/routes.js';",placement:'AFTER',payload:'forward/host/daily-rate-correction.import.jsinc'},
 {id:'DailyDispatch',anchor:'      const invoiceAsyncResponse = await handleInvoiceAsyncHttpRequest(req, env, ctx, {',placement:'BEFORE',payload:'forward/host/daily-rate-correction.dispatch.jsinc'}];
let after=before;
for(const c of changes){
 if(after.split(c.anchor).length!==2)throw Error('EXACT_CURRENT_PARENT_ANCHOR_REQUIRED');
 const newline=before.includes('\r\n')?'\r\n':'\n',payload=fs.readFileSync(path.join(here,c.payload),'utf8').replaceAll('\r\n','\n').replaceAll('\n',newline);
 c.appliedText=c.placement==='AFTER'?c.anchor+newline+payload:c.placement==='BEFORE'?payload+c.anchor:null;
 if(!c.appliedText)throw Error('BOUNDED_PARENT_POSITION_REQUIRED');after=after.replace(c.anchor,c.appliedText);
 c.anchorCount=1;c.payloadSha256=sha(fs.readFileSync(path.join(here,c.payload)));
}
let reverse=after;
for(const c of [...changes].reverse()){if(reverse.split(c.appliedText).length!==2)throw Error('PARENT_REVERSE_UNIQUE_REQUIRED');reverse=reverse.replace(c.appliedText,c.anchor);}
if(reverse!==before)throw Error('WHOLE_CURRENT_PARENT_REVERSE_FAILED');
// Syntax only: no import evaluation, no fetch/DB, no Worker launch.
try{execFileSync(process.execPath,['--check','--input-type=module'],{input:after,encoding:'utf8',windowsHide:true,maxBuffer:1024*1024});}catch{throw Error('PROPOSED_PARENT_SYNTAX_REFUSED');}
const nativeDir='supabase/repeatable/16102026_0858_nhsp_v6_daily_native_authority';
const nativeFiles=fs.readdirSync(path.join(root,nativeDir)).sort().map(f=>nativeDir+'/'+f);
const rootInputs=[parentFile,...modules,'supabase/repeatable/16102026_0858_nhsp_v6_daily_native_authority_v1.sql',...nativeFiles,
 'supabase/repeatable/includes/nhsp_v6_health_review_context.sqlinc','supabase/repeatable/includes/nhsp_v6_health_native_apply.sqlinc',
 'supabase/repeatable/includes/nhsp_v6_health_composition_wrappers.sqlinc','supabase/repeatable/14102026_0044_weekly_source_protected_pay_authority_v2.sql'];
const packFiles=['START_HERE.md',...['HEALTHROSTER_BUILDER_INSTRUCTIONS.md','FINAL_REVIEW_RECORD.md','OWNERSHIP.md',
 'Health_CODE_AUDIT.md','BANKING_CODE_AUDIT.md','HISTORY_RECONCILIATION_CONTRACT.md',
 'WORKING_PREPARATION/MODULE_AND_CLOSURE_TARGETS.json','WORKING_PREPARATION/BATCH_DECISION_INTEGRATION_DELTA_PLAN.json',
 'WORKING_PREPARATION/DAILY_RATE_CORRECTION_CONTRACT.md','WORKING_PREPARATION/DAILY_RATE_CORRECTION_CONTRACT.json'].map(f=>'NHSP_AUTHORISATION_AUDIT_15102026_HANDOFF_V6/'+f)];
const declared=[];
for(const f of nativeFiles){const text=read(root,f).toString('utf8');for(const m of text.matchAll(/create\s+or\s+replace\s+function\s+((?:private|public)\.[a-z0-9_]+)\s*\(/gi))declared.push({name:m[1],file:f});}
const signatures=['private.daily_rate_correction_public_assert_v1(jsonb,text)',
 'public.timesheet_daily_rate_correction_preview_v1(jsonb)','public.timesheet_daily_rate_correction_accept_v1(jsonb)','public.timesheet_daily_rate_correction_result_v1(jsonb)'];
const originalUiPath='docs/healthroster-build/proposals/checklist-v6-contract-correction-20261016/forward/office/js/correction-contract-v3.js';
const originalUi=execFileSync('git',['show','00f25571ac7fb65f44351a52beced0710211138e:'+originalUiPath],{cwd:root});
const originalDto=execFileSync('git',['show','00f25571ac7fb65f44351a52beced0710211138e:docs/healthroster-build/proposals/checklist-v6-contract-correction-20261016/forward/contracts/contract-correction-v3.mjs'],{cwd:root}).toString('utf8');
const exports=[...originalDto.matchAll(/export\s+(?:const|function)\s+(\w+)/g)].map(m=>m[1]);
const generated=`// Generated from sole Health closed DTO source SHA256 ${sha(originalDto)}. Do not edit.\n(function(root){'use strict';\n${originalDto.replace(/export\s+(?=(?:const|function)\b)/g,'')}\nroot.CloudTMSCorrectionContractV3=Object.freeze({${exports.join(',')}});\n})(typeof window!=='undefined'?window:globalThis);\n`;
if(!originalUi.equals(Buffer.from(generated))||originalDto.replaceAll('\r\n','\n')!==read(root,dto).toString('utf8').replaceAll('\r\n','\n'))throw Error('SOLE_GENERATED_DTO_PARENT_DRIFT');
const httpTestPath='docs/healthroster-build/proposals/checklist-v6-contract-correction-20261016/tests/daily-rate-http.test.mjs';
const originalTest=execFileSync('git',['show','00f25571ac7fb65f44351a52beced0710211138e:'+httpTestPath],{cwd:root}).toString('utf8');
const oldImport="'../../../../../broker/src/daily-rate-correction.js'",newImport="'./existing-exact/broker/src/daily-rate-correction.js'";
if(originalTest.split(oldImport).length!==2)throw Error('ORIGINAL_HTTP_TEST_IMPORT_CHANGED');
const selectedTest=originalTest.replace(oldImport,newImport);
fs.writeFileSync(path.join(here,'original-http.test.mjs'),selectedTest);
const receipt={version:'HEALTH_DAILY_PUBLIC_SERVICE_SOURCE_ASSEMBLY_V1',sourceOnly:true,rootHead:git('rev-parse','HEAD'),
 originalDailyProducerCommit:'00f25571ac7fb65f44351a52beced0710211138e',rootInputs:rootInputs.map(f=>pin(root,f)),ownerPackInputs:packFiles.map(f=>pin(pack,f)),
 currentNativeDefinitions:declared,currentNativeDefinitionCount:declared.length,
 publicAdditions:signatures,publicAcl:'three new service_role-only public RPCs; owner-only structural helper; existing0858 owners/ACL unchanged',
 rootAllocation:'Root sole integrator allocates actual managed repeatable/include paths and final registry chains; do not install as an ad hoc patch',
 parent:{file:parentFile,beforeSha256:sha(before),proposedAfterSha256:sha(after),wholeRawReverse:true,syntaxOnly:'PASS',changes},
 existingExactModules:modules.map(f=>({mode:'VERIFY_EXISTING_EXACT_NO_OVERWRITE',...pin(root,f)})),
 generatedOfficeDto:{owner:'Health sole canonical decoder; Physical mounts Office',originalProducerCommit:'00f25571ac7fb65f44351a52beced0710211138e',originalCanonicalSha256:sha(originalDto),generatedSha256:sha(originalUi),wholeGenerationExact:true,currentBrokerEquivalent:true},
 originalHttpTests:{sourceCommit:'00f25571ac7fb65f44351a52beced0710211138e',path:httpTestPath,wholeOriginalSha256:sha(originalTest),selectedSha256:sha(selectedTest),oneImportOnlyReverse:selectedTest.replace(newImport,oldImport)===originalTest,scope:'ISOLATED_TRANSPORT_MOCKS_NOT_NATIVE_RECEIPTS'},
 absentPrivateHealthDelegates:['private.daily_rate_correction_review_v1(jsonb)','private.daily_rate_correction_accept_v1(jsonb)','private.daily_rate_correction_result_v1(uuid,uuid)'],
 declaredBankInterface:{signature:'private.bpay_next_daily_rate_correction_publish_v1(uuid,uuid)',contract:'DAILY_RATE_PUBLICATION_V1',approvedSource:'V6 Daily contract only; no complete current I2 body',status:'DEPENDENCY_HELD'},
 missingFiniteEconomicInput:'Daily native paid/frozen/Summary preflight/delegate ABI and full Health private composition; Contract cut ABI is not a Daily substitute',
 effects:{businessDml:0,ddlFixtures:0,financialPublisherBodies:0,newFinancialReceipts:0},native:'NOT_RUN',freshEconomicsComplete:false};
fs.writeFileSync(path.join(here,'ASSEMBLY.json'),JSON.stringify(receipt,null,2)+'\n');
fs.writeFileSync(path.join(here,'REGISTRY_ADDITIONS.json'),JSON.stringify({mode:'ADDITIVE_FOUR_SIGNATURES_ONLY_PRESERVE_ALL_CURRENT_COMPANIONS_EDITOR_METADATA',definitions:signatures.map(signature=>({signature,physicalSource:'forward/sql/01_public_services.sqlinc',sourceSha256:sha(service),new:true,registration:'Root assigns actual finalRoot/finalPhysical from approved mount'}))},null,2)+'\n');
console.log(JSON.stringify({status:'DAILY_HEALTH_PUBLIC_SOURCE_PREPARED',definitions:signatures.length,currentNative:declared.length,parentRawReverse:true,generatedDtoExact:true,freshEconomicsComplete:false,native:'NOT_RUN'}));
