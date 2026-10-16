import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url));
const office='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-v2-office-13102026';
const pack='C:/tmp/healthroster-handover/HealthRoster_Builder_Pack_16102026/NHSP_AUTHORISATION_AUDIT_15102026_HANDOFF_V6';
const commit='045c95cecf2dcbb632c0a5827f19c38548ee3a11';
const hash=x=>createHash('sha256').update(x).digest('hex');
const git=(...args)=>execFileSync('git',args,{cwd:office,maxBuffer:64*1024*1024});
const pins=['js/main.js','index.html','js/contract-correction-v3.mjs','js/timesheet-correction-office-context.js','js/weekly-source/workspace-actions.js','js/weekly-source/import-workspace.js'].map(file=>{
 const bytes=git('show',commit+':'+file),saved=fs.readFileSync(path.join(office,file));
 return {file,bytes:bytes.length,sha256:hash(bytes),savedEqualFrozenAtReview:saved.equals(bytes)};
});
if(pins.some(x=>!x.savedEqualFrozenAtReview))throw Error('BASELINE_DRIFT');
const main=git('show',commit+':js/main.js').toString('utf8'),index=git('show',commit+':index.html').toString('utf8');
const moduleFile='WORKING_PREPARATION/MODULE_AND_CLOSURE_TARGETS.json',moduleBytes=fs.readFileSync(path.join(pack,moduleFile)),modules=JSON.parse(moduleBytes);
const authorityFiles=['FINAL_REVIEW_RECORD.md','Health_CODE_AUDIT.md','OWNERSHIP.md','MOVE_STAGE_CONTRACT.md','SOURCE_NATIVE_ACCEPTANCE_CONTRACT.md','UI_POLICY_AND_COPY.md',moduleFile,'WORKING_PREPARATION/DAILY_RATE_CORRECTION_CONTRACT.md'];
const sourcePins=authorityFiles.map(file=>({file,bytes:fs.statSync(path.join(pack,file)).size,sha256:hash(fs.readFileSync(path.join(pack,file)))}));
const symbols=['openTimesheet','renderTimesheetOverviewTab','renderTimesheetFinanceTab','showModal','closeModal'];
const existing=symbols.map(name=>({name,definitionLines:[...main.matchAll(new RegExp('^(?:async )?function '+name+'\\s*\\(','gm'))].map(m=>main.slice(0,m.index).split('\n').length)}));
const absent=['openTimesheetContractMoveReview','typedContractMove','SourceNativeComposedResultV1','CURRENT_CONTRACT_PLACEMENT_V1','TIMESHEET_CONTRACT_CORRECTION_V3','DAILY_RATE_CORRECTION_V1','CloudTMSTimesheetContractMoveV1','CloudTMSDailyRateCorrectionV1'];
if(absent.some(name=>main.includes(name)))throw Error('ABSENCE_OBSERVATION_DRIFT');
const tracked=git('ls-tree','-r','--name-only',commit,'js').toString('utf8').trim().split('\n');
const review={version:'B051_HEALTH_HOST_COVERAGE_V1',status:'INTERIM_ASSIGNED_HEALTH_IMPLEMENTATION_GAPS',office,commit,sourcePins,pins,
 reviewedPackContent:'afaf162e59b2e7901ef0dbc3f484ce51c741f0971557f80fdc877bb8b9ecaa88',selector:'B051:HEALTH_PHYSICAL_HOST_RESULT',
 exactHealthTargets:modules.cm17.hostIntegration.census_claims,existing,absentFromCompleteMain:absent,
 absentTrackedModules:['js/timesheet-contract-move.js','js/daily-rate-correction.js','js/daily-rate-correction-http-adapter.js','js/correction-contract-v3.js'].filter(file=>!tracked.includes(file)),
 currentDecoder:{file:'js/contract-correction-v3.mjs',strictSharedContractDailyResult:true,currentPlacementDecoder:true,onlyExistingRuntimeImport:'Contact manual enquiry imports validateAction; main/index have no correction decoder or host import'},
 currentLoader:{contractModule:index.includes('./js/timesheet-contract-move.js'),dailyModule:index.includes('./js/daily-rate-correction.js')},
 healthWork:[
 'Mount one actual Timesheet Overview More host; no Bulk/checking-record Move and no Daily Change Contract.',
 'Bind actual actor, native frame/context/token/open generation/stack and selected tab; route overlay Close/Escape without popping parent; dispose scoped overlay on native teardown.',
 'Use the declared CM17 open/createTimesheetOverlayAdapter ABI when its real reviewed component and native capability are supplied; never author a second Contract dialog or invent capability JSON fields.',
 'Strictly decode current placement/result; refetch actual current B immediately after genuine COMMITTED result independently of REGISTERED/PROCESSING or invoice PDF progress; preserve selected tab and immutable historical receipts.',
 'Mount the existing Health Daily UI/HTTP contribution with exact same-physical result and active native role checks, uncertainty journal and closed native dependency.',
 'Resolve only exact CommonAction typed targets into existing owners; no arbitrary URL navigation or private outer Source receipt presented as ordinary public result.'
 ],
 separateDependencies:[
 'Bank CM16 HTTP adapter/registration and CM17 universal dialog/overlay adapter, plus its bounded nested payment-summary fragment. This review does not inspect Bank source or claim producer completion/absence outside Office.',
 'Actual authenticated public native Preview/current result/capability projection; Source lane owns READ_RESULT and SourceNativeComposedResultV1 source closure.',
 'Native/installed/role/concurrency/financial/document/API/phone acceptance and final Part B remain separate.'
 ],
 preservedRules:['Exact 045c Manual New Enquiry parent','Independent Source versus Banking pending/history','Current B visible without all-PDF/all-APPLIED gate','No fresh Move repair wizard or mandatory reason essay','Source original invoice/member/amount/document unchanged','Issued originals/credits and original paid/frozen history remain immutable; no automatic Issue/Unissue/email','A→B→A requires genuine new typed acknowledgement linked to immutable A; never resurrect an old receipt'],
 native:'NOT_RUN',partB:'NOT_RUN',bankComponentAudit:false,productEditsByThisReview:false};
fs.writeFileSync(path.join(here,'REVIEW.json'),JSON.stringify(review,null,2)+'\n');
console.log(JSON.stringify({status:review.status,pins:pins.length,authorityPins:sourcePins.length,existing:existing.length,absent:absent.length,bankComponentAudit:false}));
