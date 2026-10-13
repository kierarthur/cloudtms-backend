import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),lane=path.resolve(here,'../../../..'),root=process.argv[2];
assert.ok(root,'explicit controlling root required');
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const fileFact=(base,file)=>{const b=fs.readFileSync(path.join(base,file));return {path:file,sha256:sha(b),bytes:b.length};};
const write=(file,value)=>fs.writeFileSync(path.join(here,file),typeof value==='string'?value:JSON.stringify(value,null,2)+'\n');
const folder='docs/healthroster-build/proposals/cn-a0-native-normalisation';
const guardFiles=['ADAPTER_SOURCE_GUARD.json','APPROVAL_SOURCE_GUARD.json','WORKER_SOURCE_GUARD.json','NATIVE_EXTRACTION_SOURCE_GUARD.json'];
for(const name of guardFiles){const g=JSON.parse(fs.readFileSync(path.join(here,name)));assert.equal(fileFact(root,g.input.file??g.input.path).sha256,g.input.sha256,'root input changed '+name);}
const protectedInputs=[];
for(const [file,expected,count] of [
 ['docs/healthroster-build/PROTECTION_SOURCE_MANIFEST.json','2d0947db59f875da1fd4aeed4b49c6566174b19cc244c41785670248c3fd1070',52],
 ['docs/healthroster-build/proposals/f4-ai/F4_AI_SOURCE_MANIFEST.json','6de00fa2d72625b8b3d39b38b14dd4765ab76989ae297abfcf2c97b7ae5010a8',45]]){
 const b=fs.readFileSync(path.join(lane,file));assert.equal(sha(b),expected);const m=JSON.parse(b);assert.equal(m.files.length,count);
 for(const f of m.files)assert.equal(fileFact(lane,f.path).sha256,f.sha256,'frozen handoff changed '+f.path);
 protectedInputs.push({...fileFact(lane,file),pinnedFiles:count,status:'UNCHANGED'});
}
const contractFile='supabase/release/current-contract.json',contract=JSON.parse(fs.readFileSync(path.join(root,contractFile)));
const relationFields=new Map(contract.relations.filter(r=>r.columns).map(r=>[r.schema+'.'+r.name,new Set(r.columns.map(c=>c.name))]));
const requestMigration='supabase/migrations/13102026_1127_healthroster_timesheet_correction_requests_v1.sql';
const rulesMigration='supabase/migrations/13102026_1133_weekly_source_rule_sets_schema.sql';
const releaseMigration='supabase/migrations/22082026_1507_cloudtms_database_release_control_plane.sql';
function declaredFields(file,relation){
 const text=fs.readFileSync(path.join(root,file),'utf8');
 const table=text.match(new RegExp('create table(?: if not exists)? '+relation.replace('.','\\.')+' \\(([\\s\\S]*?)\\n\\);','i'));
 assert.ok(table,relation);return new Set([...table[1].matchAll(/^\s{2}([a-z_]\w*)\s+(?:uuid|text|bigint|integer|boolean|date|jsonb|timestamptz)\b/gm)].map(m=>m[1]));
}
relationFields.set('private.timesheet_correction_requests',declaredFields(requestMigration,'private.timesheet_correction_requests'));
relationFields.set('public.weekly_source_group_rule_assignments',declaredFields(rulesMigration,'public.weekly_source_group_rule_assignments'));
relationFields.set('private.cloudtms_database_releases',declaredFields(releaseMigration,'private.cloudtms_database_releases'));
const scope=fs.readFileSync(path.join(here,'13102026_2002_a0_clock_scope.sqlinc'),'utf8');
const aliases={v_request:'private.timesheet_correction_requests',r:'private.timesheet_correction_requests',
 v_ts:'public.timesheets',t:'public.timesheets',v_contract:'public.contracts',c:'public.contracts',
 f:'public.timesheets_financials',pf:'public.timesheets_financials',s:'public.nhsp_shifts',l:'public.weekly_source_row_timesheet_lineages',
 e:'public.candidate_expense_components',g:'public.weekly_source_groups',gc:'public.weekly_source_group_clients',
 a:'public.weekly_source_group_rule_assignments',rel:'private.cloudtms_database_releases'};
const masked=scope.replace(/--[^\n]*/g,'').replace(/'(?:''|[^'])*'/g,"''");
const census=Object.entries(aliases).map(([alias,relation])=>{
 const fields=[...new Set([...masked.matchAll(new RegExp('\\b'+alias+'\\.([a-z_]\\w*)','g'))].map(m=>m[1]))].filter(f=>!(alias==='s'&&f==='value')).sort();
 const cols=relationFields.get(relation);assert.ok(cols,relation);const missing=fields.filter(f=>!cols.has(f));assert.deepEqual(missing,[],relation);
 return {alias,relation,fields,missing,columnSource:relation.startsWith('private.')||relation==='public.weekly_source_group_rule_assignments'?'DECLARED_MIGRATION_SOURCE':'CURRENT_GENERATED_CONTRACT'};
});
write('A0_SCOPE_SCHEMA_CENSUS.json',{contract:'HEALTHROSTER_A0_CLOCK_SCOPE_COLUMN_CENSUS_V1',status:'SOURCE_SCHEMA_NOT_RUNTIME',
 inputs:[contractFile,requestMigration,rulesMigration,releaseMigration].map(f=>fileFact(root,f)),census,
 limitation:'Includes every explicit native relation/rowtype alias in the new scope owner. Source declared private/F1 relations remain actual catalogue gates; CREATE, source hashes and column census do not prove runtime function/trigger/enum/API behaviour.'});
write('A0_CLOCK_ARM_REVIEW.md',`# Distinct exact-request A0 clock arm

Every native route remains HELD for root's real first-use fixture. This unmounted source packet grants no activation, financial/document/NEXT acceptance or readiness.

The ordinary HealthRoster/NHSP no-import worker calls unchanged rebuildFromExistingSegmentsEvidence and refuses NO_EXISTING_SEGMENTS_EVIDENCE when Original has no FIN/segments. The extracted ordinary dispatcher retains that refusal. The proposed complete factory now binds exact-request-a0-weekly-clock-arm.mjs for server-owned missing Original purpose; ordinary/P0 purposes delegate to unchanged dispatch. The arm calls only existing buildWeeklyScheduleSegmentsSnapshot with write_now:false, no queue ID, immutable original actual clocks/settings and native dated policy. It calculates no alternative finance.

The complete private timesheet_correction_a0_clock_scope_v1(uuid) owner qualifies an exact WAITING signed request, physical Original/book/Contract/Candidate/Client/version, real signed witness, each worked date inside its Contract/week, and actual frozen settings integrity. It requires no FIN for Original, no active imported shifts or physical Source lineage and no unmaterialised unremoved expense components on Original/P0. The unchanged weekly calculator preserves expenses/mileage from curFin; a native claim may never silently become zero.

Admission reuses actual weekly_source_signed_check_only_v1 and _timesheet_settings_authority_frozen_v1 plus one exact active dated ROSTER membership and enabled SIGNED_CHECK_ONLY NULL-rule assignment, its native activation receipt and recorded VERIFIED release. Applicable activated RULE_SET history, including disabled assignments, refuses. A NULL effective-rule result alone grants nothing. The closed INTERNAL DTO binds exact native Contract input, Timesheet/settings hashes, activation receipt and decimal-text group/assignment versions. Current stored Contract rates are the unchanged native CONTRACT_RATES_JSON producer input; no historical rate is invented or certified.

The complete1127 successor adds this scope only for missing weekly Original before hashing PREPARE. Its existing atomic owner re-PREPAREs under the existing request/workflow/CW/TS/FIN locks and compares that fingerprint before writing. Existing-finance and Daily PREPARE shapes stay unchanged. SQL normal-writes Original, rereads its actual stored output and signs that output for R0 in the same unit. It never requires calculator hash to equal normalized stored hash.

Source-priced, enabled Rule and unproved historical pricing routes remain refused. Current agency SEND requires an ISSUED/PAID original invoice, while the new clock arm requires no original/return invoice and an actual signed NULL-rule scope. A synthetic uninvoiced agency fixture cannot qualify agency missing-A0. Existing agency finance retains the frozen Original path. Unmaterialised native expense/mileage claims require their actual owner journey and are held. No business ruling or store changes.

The new scope reader adds no group/assignment/Contract locks. Readonly fingerprint CAS alone does not close races after its recheck. Root must compose and qualify the full Candidate/I-1/group/Contract/source graph, imported/expense evidence phantoms and Contract/assignment changes before mounting or activation. SQL canonical compilation, generated contract/owner-only ACL, latest release/application readiness, real financial full-component cancellation, actual document pages, NEXT, TestFlight and phone proof remain root gates.

Seven clock scope/caller tests use synthetic closed native-wire metadata and mocks, never a fabricated database VERIFIED record. Required native first use includes an actually activated signed NULL-rule Original; real FIN/import/lineage/claim absence; Rule/SOURCE/disabled-history/agency-issued-parent negatives; actual Contract rates/channel/additional/UK break/overnight authority; actual stored normalisation and full Original/R0 component/VAT/expense cancellation; same-unit rollback/joined concurrency; existing FIN and ordinary parity. No database/client/email or hosted action was performed by this lane.
`);
let wire=fs.readFileSync(path.join(here,'A0_NATIVE_WIRE_AUDIT.md'),'utf8');
if(!wire.includes('| Optional authority.original_a0_clock_scope |'))wire=wire.replace('| Notification IDs | Exactly physical P0 | No notification for Original or R0 is invented. |',
 '| Notification IDs | Exactly physical P0 | No notification for Original or R0 is invented. |\n| Optional authority.original_a0_clock_scope | Closed INTERNAL TIMESHEET_CORRECTION_A0_CLOCK_SCOPE_V1 | Missing weekly Original only. Actual signed NULL-rule/settings/Contract/absence witness participates in PREPARE fingerprint; no Candidate/MyTMS exposure or readiness claim. |');
wire=wire.replace('The complete approval successor changes only the two original/P0 input assertions and adds strict stored-row rereads after each bounded write.',
 'The complete approval successor changes the two original/P0 input assertions, adds strict stored-row rereads after each bounded write and binds the exact missing-weekly-Original pricing scope into PREPARE CAS.');
wire=wire.replace(/Integration order:[\s\S]*?\n\nThe focused suites/,`Integration order: add both new private owners (canonical_input_assert and a0_clock_scope) to root's owner-only registry and define them before complete1127 PREPARE/approval. Add the complete adapter, producer modules/factory and guarded index hunks only with actual qualified actor/RPC/request-read/cleanup/ordinary adapters. The distinct missing-A0 arm has concrete signed NULL-rule qualification; ordinary/P0 dispatch keeps its native no-segments refusal. Source/Rule, agency-issued-parent missing finance and unmaterialised expense authority remain held. Read A0_CLOCK_ARM_REVIEW.md for exact admission and the remaining lock graph. Root owns generated contract, browser/service isolation, actual latest release, application/NEXT and native financial/doc/client proof. Source presence and readonly CAS do not prove readiness or concurrency. Native PREPARE recheck and canonical writer policy assertion remain mandatory.\n\nThe focused suites`);
wire=wire.replace('59 tests: 42 CN callback/native-wire cases and 17 producer/route/context cases.',
 '66 tests: 42 CN callback/native-wire cases, 17 producer/route/context cases and 7 distinct clock scope/caller cases.');write('A0_NATIVE_WIRE_AUDIT.md',wire);
const journey=JSON.parse(fs.readFileSync(path.join(here,'NATIVE_JOURNEY_MANIFEST.json')));
journey.status='COMPLETE_SOURCE_PACKET_UNMOUNTED_ALL_NATIVE_ROUTES_HELD';
journey.journeys.forEach(j=>j.native_status='HELD_ACTUAL_FIRST_USE_NOT_RUN');
const agency=journey.journeys.find(j=>j.case==='AGENCY_A0_WEEKLY_MISSING_FIN');
agency.source='Current actual agency SEND requires ISSUED/PAID parent and nonnull original_invoice_id. No native missing-finance parent reconstruction is proved.';
agency.oracle='Refuse the new missing-finance clock arm; never qualify an uninvoiced synthetic agency fixture. Existing agency FIN uses unchanged frozen Original/sign-copy path.';
journey.scope_boundary={owner:'private.timesheet_correction_a0_clock_scope_v1(uuid)',admitted_source_proposal:'Exact signed NULL-rule CHECK_ONLY Contract clocks with controlled activation/settings/dates and absent FIN/import/lineage/unmaterialised claims',
 withheld:['SOURCE_AMOUNT / enabled Rule pricing','Disabled activated Rule history or ambiguous dated source scope','Agency issued-parent with missing FIN','Unmaterialised native expense/mileage claim','Unknown/missing immutable settings, signed evidence, native rates/channel/additional authority'],
 concurrency:'Readonly PREPARE CAS does not replace the pending complete Candidate/I-1/group/Contract/source locks and phantom proof.'};
const newGates=['Actual signed NULL-rule activation/settings/Contract scope owner first use and private owner/schema checks',
 'SEND7 root-held shared turn; Banking first install before full rehearsal; no lane database calls'];
journey.pending_gates=[newGates[0],...journey.pending_gates.filter(g=>!newGates.includes(g)),newGates[1]];write('NATIVE_JOURNEY_MANIFEST.json',journey);
const tests=['tests/healthroster-cn-a0-callbacks.test.mjs','tests/healthroster-cn-a0-producers.test.mjs','tests/healthroster-cn-a0-clock-scope.test.mjs'];
const jsFiles=fs.readdirSync(here).filter(n=>/\.(?:mjs|js)$/.test(n)).map(n=>path.join(here,n)).concat(tests.map(f=>path.join(lane,f)));
for(const file of jsFiles){const r=spawnSync(process.execPath,['--check',file],{encoding:'utf8',windowsHide:true});assert.equal(r.status,0,r.stderr);}
for(const file of ['canonical-input-assert.sqlinc','13102026_2002_a0_clock_scope.sqlinc','13102026_1127_approval.complete-proposed.sqlinc']){
 const s=fs.readFileSync(path.join(here,file),'utf8');assert.doesNotMatch(s,/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
 assert.equal((s.match(/\$function\$/g)||[]).length%2,0);}
for(const file of ['ADAPTER_ROOT.diff','APPROVAL_ROOT.diff','WORKER_ROOT.diff']){
 const r=spawnSync('git',['apply','--check',path.join(here,file)],{cwd:root,encoding:'utf8',windowsHide:true});assert.equal(r.status,0,r.stderr);}
const results=spawnSync(process.execPath,['--test',...tests],{cwd:lane,encoding:'utf8',windowsHide:true,maxBuffer:4*1024*1024});
assert.equal(results.status,0,results.stdout+results.stderr);assert.match(results.stdout,/tests 66/);assert.match(results.stdout,/pass 66/);
write('SOURCE_CHECKS.json',{contract:'HEALTHROSTER_CN_A0_SOURCE_CHECKS_V1',status:'PASS_SOURCE_ONLY',
 tests:{total:66,passed:66,failed:0,files:tests},javascriptSyntaxFiles:jsFiles.length,rootPatchApplyChecks:3,
 sqlStaticGuards:'Illegal schema-qualified syntax constructs and delimiter pairing only; no PostgreSQL compilation/runtime claim.',
 currentRowtypeCensus:{generatedContractScopes:47,declaredRequestScopes:20,missing:0,unmapped:0},newScopeColumnCensus:{aliases:census.length,missing:0},
 frozenHandoffs:protectedInputs,database:false,docker:false,hosted:false,appBuild:false,externalEmail:false,
 withheld:journey.pending_gates});
const inputs=[...new Set([
 'broker/src/index.js','broker/src/timesheet-correction-orchestrator.mjs','broker/src/daily-canonical-authority.js',
 'supabase/repeatable/13102026_1127_healthroster_timesheet_correction_requests_v1.sql',
 ...fs.readdirSync(path.join(root,'supabase/repeatable/13102026_1127_healthroster_correction_requests')).filter(f=>f.endsWith('.sqlinc')).map(f=>'supabase/repeatable/13102026_1127_healthroster_correction_requests/'+f),
 ...fs.readdirSync(path.join(root,'supabase/repeatable/13102026_1143_healthroster_scoped_credit')).filter(f=>f.endsWith('.sqlinc')).map(f=>'supabase/repeatable/13102026_1143_healthroster_scoped_credit/'+f),
 'supabase/repeatable/13102026_1134_weekly_source_rule_sets_v1.sql',
 'supabase/repeatable/03092026_1641_contract_settings_effective_authority_v1.sql',
 'supabase/repeatable/21072026_1235_36_tsfin_load_context_batch.sql','supabase/repeatable/06012026_tsfin_load_weekly_context_batch.sql',
 'supabase/repeatable/06012026_tsfin_load_nhsp_shifts_batch.sql',
 'supabase/repeatable/21072026_1235_37_tsfin_write_current_snapshot_single_bounded.sql',
 'supabase/repeatable/21072026_1235_38_tsfin_write_snapshots_and_complete.sql',
 'supabase/repeatable/21072026_1235_00_import_correction_policy_helpers.sql',
 contractFile,requestMigration,rulesMigration,releaseMigration
 ])].map(f=>fileFact(root,f));
const names=fs.readdirSync(here).filter(n=>n!=='A0_SOURCE_MANIFEST.json');
const files=names.map(n=>fileFact(lane,folder+'/'+n)).concat(tests.map(f=>fileFact(lane,f))).sort((a,b)=>a.path.localeCompare(b.path));
const rootHead=spawnSync('git',['rev-parse','HEAD'],{cwd:root,encoding:'utf8',windowsHide:true});assert.equal(rootHead.status,0);
const rootBranch=spawnSync('git',['branch','--show-current'],{cwd:root,encoding:'utf8',windowsHide:true});assert.equal(rootBranch.status,0);
write('A0_SOURCE_MANIFEST.json',{contract:'HEALTHROSTER_CN_A0_COMPLETE_SOURCE_PACKET_V1',status:'FROZEN_SOURCE_FOR_ROOT_REVIEW_NOT_INTEGRATED_OR_NATIVE_ACCEPTED',
 authoritativeLane:{branch:'codex/healthroster-v2-protection-build-13102026',repository:'kierarthur/cloudtms-backend',sourceOnly:true},
 rootInputPath:root,rootInputGitHead:rootHead.stdout.trim(),rootInputBranch:rootBranch.stdout.trim(),
 completeRepositoryBoundary:'Own branch preserves the complete original repository and frozen handoffs. This unmounted proposal targets the current complete root repository at the recorded HEAD plus exact50 saved input hashes; source builders/hunks refuse changed controlling input. Root integrates into and backs up its complete repository before runtime publication. No temporary build copy or standalone app is authority.',
 inputs,files,protectedInputs,
 newPrivateOwners:['private.timesheet_correction_canonical_input_assert_v1(jsonb,uuid,integer,uuid,uuid)','private.timesheet_correction_a0_clock_scope_v1(uuid)'],
 integration:{adapter:'Guarded complete adapter e51 predecessor; eight bounded native-wire hunks',approval:'Guarded complete1127 owner; five hunks, both private includes before PREPARE/atomic',
  nativeWorker:'Guarded existing index input; two additive factory/import hunks, no route mounted',nativeBodies:'Call-only ordinary calculators, native single/batch writers/sign-copy/approval/outbox/cleanup/lifecycle owners; no Banking edits'},
 invariants:['No new correction/finance/payment/query store or calculator','Exact awc TEXT/booking identity, SHA target revision and proposal revision1',
  'Missing PREPARE nullable authorities match actual native shape','Actual stored Original reread/sign-copy in same native atomic unit',
  'Existing FIN immutable hash and exact original/R0 reversal retained','Fresh APPLIED materialisation fact, actual FIN IDs/hashes and exactly3/2 approved members',
  'P0-only notification, real manager authority, CAS/replay/cancel/return/cleanup and complete lifecycle guards',
  'No Candidate/MyTMS72-operation/auth/manager DTO changes; ordinary TSFIN dispatch/refusal preserved','Policy X call-only native boundaries; no post-draft live identity fallback'],
 checks:{tests:66,pass:66,javascriptSyntaxFiles:jsFiles.length,patchApplyChecks:3,rootEdits:false,frozen97FilesUnchanged:true},
 withheldScopes:journey.scope_boundary.withheld,gates:journey.pending_gates,
 database:false,docker:false,hostedMutation:false,build:false,email:false,gitBackup:'Reviewed own feature branch under SEND5B; fresh exact remote identity reported separately, no promotion.'});
console.log(JSON.stringify({manifest:fileFact(lane,folder+'/A0_SOURCE_MANIFEST.json'),files:files.length,inputs:inputs.length,
 tests:66,jsSyntax:jsFiles.length,rootPatchChecks:3,frozenFilesUnchanged:97}));
