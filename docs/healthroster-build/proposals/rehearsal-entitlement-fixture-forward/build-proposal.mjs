// Isolated source generation and trace. Never opens a database or runs a harness.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),lane=path.resolve(here,'../../../..');
const root=path.resolve(lane,'../healthroster-v2-backend-13102026');
const COMMIT='547c74da5f0a05b09297c042bc7f3cc0c0105b85',pins=[],hunks=[];
const hash=x=>crypto.createHash('sha256').update(x).digest('hex');
function read(file){const bytes=fs.readFileSync(path.join(root,file));pins.push({path:file,bytes:bytes.length,sha256:hash(bytes)});return bytes.toString('utf8');}
function change(text,before,after,file){if(text.split(before).length!==2)throw new Error('HUNK_CARDINALITY '+file);hunks.push({file,before,after});return text.replace(before,()=>after);}
const names=['17092026_0100_weekly_source_entitlement_schema_v1.sql','17092026_0600_weekly_source_first_authorisation_v1.sql',
  '17092026_0610_weekly_source_withdrawal_supersession_v1.sql','17092026_0800_weekly_source_workbench_seams_v1.sql'];
const originals=names.map(name=>read('supabase/verification/'+name));
const gitPins=names.map((name,i)=>{const bytes=execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','show',COMMIT+':supabase/verification/'+name],{cwd:root});
  return {path:'supabase/verification/'+name,commit:COMMIT,bytes:bytes.length,sha256:hash(bytes),matches_current_disk:bytes.equals(Buffer.from(originals[i]))};});
const receipt=JSON.parse(read('docs/healthroster-build/rehearsals/13102026/FULL_REHEARSAL_1_RECEIPT.json'));
const plan=JSON.parse(read('docs/healthroster-build/rehearsals/13102026/FULL_REHEARSAL_EXACT_SOURCE_PLAN.json'));
const execution=JSON.parse(read('docs/healthroster-build/rehearsals/13102026/FULL_REHEARSAL_EXECUTION_MANIFEST.json'));
if(plan.commit!==COMMIT||receipt.commit!==COMMIT)throw new Error('EXACT_REHEARSAL_COMMIT_REQUIRED');
const executed=new Set(execution.units.map(x=>x.file));
const ordered=[...plan.units,...plan.verifierUnits.filter(x=>executed.has(x.file))];
let value=null;const variableTrace=[];
for(const unit of ordered){for(const [i,line] of unit.sql.split('\n').entries()){
  if(!/^\s*\\(?:set|unset|if)\b.*weekly_source_verification_outer_transaction/.test(line))continue;
  const before=value;if(/^\s*\\set\s+weekly_source_verification_outer_transaction\b/.test(line))value=line.trim().split(/\s+/).slice(2).join(' ');
  if(/^\s*\\unset\s+weekly_source_verification_outer_transaction\b/.test(line))value=null;
  variableTrace.push({file:unit.file,rendered_line:i+1,meta:line.trim(),exists_before:before!==null,value_before:before,exists_after:value!==null,value_after:value,
    database_savepoint_rollback_restores_psql_variable:false});
}}
const affected=receipt.errors.filter(e=>names.some(n=>e.file.endsWith(n)));
fs.writeFileSync(path.join(here,'EXACT_REHEARSAL_TRACE.json'),JSON.stringify({schema_version:'ENTITLEMENT_REHEARSAL_TRACE_V1',commit:COMMIT,
  receipt_status:receipt.status,rolled_back:receipt.rolledBack,fresh_rollback_proof:receipt.connectedFresh,
  affected_errors:affected,psql_variable_trace:variableTrace,original_fixture_commit_pins:gitPins,
  source_only:true,actual_wrong_sqlstate_detail_retained:false,constraint_mode_at_failed_statement:'NOT_OBSERVED'},null,2)+'\n');
const diagnoses=affected.map(error=>{let firstCause,classification,confidence,dependsOn=[];
  if(error.file.includes('_0100_')){
    firstCause='PSQL_OUTER_VARIABLE_LEAK_SKIPS_FIXTURE_CAPTURE';confidence='EXACT_RENDERED_META_TRACE';
    classification=error.statement===24||error.statement===31?'FIRST_ROOT_OR_DIRECT_SAME_ROOT':'FIXTURE_PARENT_CASCADE';
    if(error.statement===25||error.statement===27)dependsOn=[24];
    if(error.statement===26)dependsOn=[24,25];
    if(error.statement===28||error.statement===29)dependsOn=[27];
  }else if(error.file.includes('_0600_')){
    firstCause=error.statement===43?'D8_COMMITTED_HEAD_HAS_NO_MATCHING_SCHEMA_RECEIPT':'ORDERING_FIXTURE_ASSUMES_CALLER_FRAME_IS_TOP_LEVEL';
    classification='INDEPENDENT_FIXTURE_ROOT_CANDIDATE';confidence=error.statement===43?'MISSING_RECEIPT_SOURCE_PROVEN':'FRAME_INCOMPATIBILITY_SOURCE_PROVEN_ACTUAL_ASSERTION_LABEL_WITHHELD';
  }else if(error.file.includes('_0610_')){
    if([19,20,26].includes(error.statement)){firstCause='NONZERO_SCHEMA_HEAD_BUILD_ASSUMES_DEFERRED_TIMING';classification='INDEPENDENT_FIXTURE_ROOT_CANDIDATE';confidence='CONSTRUCTION_SOURCE_PROVEN_ORIGINAL_MODE_UNOBSERVED';}
    else {firstCause=error.statement===23?'MISSING_NATIVE_WITHDRAWAL_RESULT_FROM_SAFE_CASE':'MISSING_FINANCIAL_EFFECT_CASE_PREREQUISITES';classification='FIXTURE_PARENT_CASCADE';confidence='VISIBLE_SOURCE_DEPENDENCY';dependsOn=error.statement===23?[19]:[20];}
  }else {firstCause=error.statement===19?'NONZERO_SCHEMA_HEAD_BUILD_ASSUMES_DEFERRED_TIMING':'MISSING_ONE_HEAD_FINGERPRINT_BASELINE';classification=error.statement===19?'INDEPENDENT_FIXTURE_ROOT_CANDIDATE':'FIXTURE_PARENT_CASCADE';confidence=error.statement===19?'CONSTRUCTION_SOURCE_PROVEN_ORIGINAL_MODE_UNOBSERVED':'VISIBLE_SOURCE_DEPENDENCY';if(error.statement===20)dependsOn=[19];}
  return {...error,classification,first_cause:firstCause,confidence,depends_on_statements:dependsOn,native_fix_proved:false,production_owner_defect_claimed:false};
});
fs.writeFileSync(path.join(here,'ERROR_CLASSIFICATION.json'),JSON.stringify({schema_version:'ENTITLEMENT_REHEARSAL_ERROR_CLASSIFICATION_V1',commit:COMMIT,errors:diagnoses,
  no_contended_guard_reclassified_as_defect:true,actual_first_assertion_labels:'WITHHELD_ORIGINAL_RECEIPT',runtime:'HELD_SEND_7'},null,2)+'\n');
// Preserve the original entitlement verifier conditional contract byte-for-byte.
fs.writeFileSync(path.join(here,'01_unchanged_entitlement_schema.sqlinc'),originals[0]);

// Whole guarded historical runner successor; this cannot be executed. Root must
// rebuild the current plan/identity/SEND_7 ownership, never resume old source.
let runner=read('docs/healthroster-build/rehearsals/13102026/run_full_upgrade_rehearsal.mjs');
runner=change(runner,"if(fixture)append('savepoint hr_verifier_'+i+';');append(u.sql,u);if(fixture)append('rollback to savepoint hr_verifier_'+i+';\\nrelease savepoint hr_verifier_'+i+';');",
  "if(fixture){append('savepoint hr_verifier_'+i+';');append('\\\\unset weekly_source_verification_outer_transaction');}append(u.sql,u);if(fixture){append('rollback to savepoint hr_verifier_'+i+';\\nrelease savepoint hr_verifier_'+i+';');append('\\\\unset weekly_source_verification_outer_transaction');}",'02_archived_runner_variable_scope_successor.mjs');
fs.writeFileSync(path.join(here,'02_archived_runner_variable_scope_successor.mjs'),runner);

const namedChecks=`  set constraints
    public.weekly_source_entitlement_head_inventory_assert,
    public.weekly_source_entitlement_head_component_inventory_assert,
    public.weekly_source_entitlement_head_receipt_assert`;
const buildTiming=namedChecks+' deferred;\n';
const completeTiming=namedChecks+' immediate;\n'+buildTiming;

// Historical schema fixtures remain named schema seeds. These are not native
// financial/manager/transport acceptance and cannot satisfy runtime gates.
let auth=originals[1];const authFile='03_complete_first_authorisation_fixture.sqlinc';
const authNl=auth.includes('\r\n')?'\r\n':'\n',authLines=t=>t.replace(/\n/g,authNl);
const d8Start=auth.indexOf('do $verify_d8_rules$'),d8End=auth.indexOf('$verify_d8_rules$;',d8Start);
const d8Before=auth.slice(d8Start,d8End);let d8=d8Before;
const d8Head=d8Before.match(/  insert into public\.weekly_source_entitlement_heads\(\r?\n    id,authority_kind,agency_id,candidate_id,contract_id,week_ending_date,/)?.[0];
if(!d8Head)throw new Error('D8_HEAD_SITE_MISSING');
const matchingReceipt=`  -- Named minimum schema receipt for this synthetic certified-zero head.
  -- It proves the existing FK/digest/token/head-membership prerequisite only;
  -- it is not manager, transport, applied financial or native publisher proof.
  insert into private.weekly_source_entitlement_publication_receipts(
    decision_bundle_id,bundle_revision,request_digest,publication_mode,candidate_id,
    member_root_ids,member_family_booking_ids,member_root_versions,head_ids,
    scope_change_tx_token,decision_id,decided_by_user_id,census_json,proof_json
  ) values (
    'b7000000-0000-4000-8000-000000000fb1',1,
    pg_catalog.sha256(pg_catalog.convert_to('receipt','UTF8')),'IMMEDIATE',
    'b7000000-0000-4000-8000-000000000106',
    array['b7000000-0000-4000-8000-000000000306']::uuid[],array['WP07-BK-06']::text[],
    array[2]::integer[],array['b7000000-0000-4000-8000-000000000fc1']::uuid[],
    'b7000000-0000-4000-8000-000000000fe1','b7000000-0000-4000-8000-000000000fd1',
    'b7000000-0000-4000-8000-000000000001','{}'::jsonb,'{}'::jsonb
  );
`;
const d8Finish="    'b7000000-0000-4000-8000-000000000fe1');";
if(d8.split(d8Finish).length!==2)throw new Error('D8_HEAD_FINISH_DRIFT');
const d8HeadBlock=d8.slice(d8.indexOf(d8Head),d8.indexOf(d8Finish)+d8Finish.length);
if(d8.split(d8HeadBlock).length!==2)throw new Error('D8_HEAD_SITE_DRIFT');
d8=d8.replace(d8HeadBlock,()=>authLines(matchingReceipt)+d8HeadBlock+authLines('\n'+completeTiming));
auth=change(auth,d8Before,d8,authFile);
const orderingStart=auth.indexOf('do $verify_ordering_rule$'),orderingEnd=auth.indexOf('$verify_ordering_rule$;',orderingStart);
const orderingBefore=auth.slice(orderingStart,orderingEnd);let ordering=orderingBefore;
ordering=ordering.replace('  v_xid_after bigint;',()=>authLines('  v_xid_after bigint;\n  v_owner_frame_xid text;\n  v_probe_id uuid:=pg_catalog.gen_random_uuid();'));
ordering=ordering.replace('  v_xid_before:=pg_catalog.pg_current_xact_id()::text::bigint;',()=>authLines(`  -- Observe this caller's actual SQL frame, which can be the harness's
  -- enclosing SAVEPOINT. Top-level pg_current_xact_id alone cannot name it.
  insert into pg_temp.wp07_ordering_owner_frame_probe(probe_id) values(v_probe_id)
    returning xmin::text into v_owner_frame_xid;
  v_xid_before:=pg_catalog.pg_current_xact_id()::text::bigint;`));
ordering=ordering.replace('and v_xmin_authorisation::bigint=v_xid_before,',()=> 'and v_xmin_authorisation=v_owner_frame_xid,');
ordering=ordering.replace("||' top-level xid='||v_xid_before::text);",()=> "||' own frame xid='||v_owner_frame_xid||' top-level xid='||v_xid_before::text);");
ordering=ordering.replace('that xid must be the top-level one.',()=> 'that xid must be the caller frame observed below, including an enclosing savepoint.');
auth=change(auth,orderingBefore,authLines('create temporary table wp07_ordering_owner_frame_probe(probe_id uuid primary key) on commit drop;\n\n')+ordering,authFile);
fs.writeFileSync(path.join(here,authFile),auth);

let withdrawal=originals[2];const withdrawalFile='04_complete_withdrawal_fixture.sqlinc';
const publishStart=withdrawal.indexOf('create function pg_temp.publish_head('),publishEnd=withdrawal.indexOf('$function$;',publishStart);
const publishBefore=withdrawal.slice(publishStart,publishEnd);let publish=publishBefore;
const withdrawalNl=withdrawal.includes('\r\n')?'\r\n':'\n',withdrawalLines=t=>t.replace(/\n/g,withdrawalNl);
publish=publish.replace('  insert into public.weekly_source_entitlement_decision_bundles(',()=>withdrawalLines(buildTiming)+'  insert into public.weekly_source_entitlement_decision_bundles(');
publish=publish.replace('   where root_timesheet_id=p_root and withdrawn_at_utc is null;',()=> '   where root_timesheet_id=p_root and withdrawn_at_utc is null;'+withdrawalLines('\n'+completeTiming));
withdrawal=change(withdrawal,publishBefore,publish,withdrawalFile);
fs.writeFileSync(path.join(here,withdrawalFile),withdrawal);

let workbench=originals[3];const workbenchFile='05_complete_workbench_fixture.sqlinc';
const seedStart=workbench.indexOf('create function pg_temp.seed_head('),seedEnd=workbench.indexOf('$seed$;',seedStart);
const seedBefore=workbench.slice(seedStart,seedEnd);let seed=seedBefore;
const workbenchNl=workbench.includes('\r\n')?'\r\n':'\n',workbenchLines=t=>t.replace(/\n/g,workbenchNl);
seed=seed.replace('  -- 1. the decision bundle revision',()=>workbenchLines(buildTiming)+'  -- 1. the decision bundle revision');
seed=seed.replace('  return p_head_id;',()=>workbenchLines(completeTiming)+'  return p_head_id;');
workbench=change(workbench,seedBefore,seed,workbenchFile);
fs.writeFileSync(path.join(here,workbenchFile),workbench);

const guards=read('supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql');
read('supabase/repeatable/04102026_0556_banking_pay_next_source_factual_legacy_boundary_v1.sql');
read('supabase/repeatable/17092026_0600_weekly_source_first_authorisation_v1.sql');
read('supabase/repeatable/04102026_2120_weekly_source_approval_basis_v2.sql');
read('supabase/repeatable/17092026_0800_weekly_source_mode_a_dispatch_v1.sql');
read('supabase/verification/support/22092026_1850_source_fixture_capture.sql');
read('scripts/cloudtms-db-component-release.mjs');
if(!guards.includes('WEEKLY_SOURCE_HEAD_INVENTORY_MISMATCH')||!guards.includes('WEEKLY_SOURCE_HEAD_RECEIPT_MISSING'))throw new Error('CONTROLLING_GUARDS_MISSING');
fs.writeFileSync(path.join(here,'CONTROLLING_INPUTS.json'),JSON.stringify({schema_version:'ENTITLEMENT_FIXTURE_INPUTS_V1',inputs:pins},null,2)+'\n');
fs.writeFileSync(path.join(here,'FORWARD_HUNKS.json'),JSON.stringify(hunks,null,2)+'\n');
console.log(JSON.stringify({exact_commit:COMMIT,affected_errors:affected.length,psql_meta_sites:variableTrace.length,entitlement_schema_unchanged:true,complete_fixture_successors:3,runner_guard_preserved:true}));
