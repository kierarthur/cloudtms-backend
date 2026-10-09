// HELD E-only diagnostic. No driver import, CLI dispatch, connection, or owner install.
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createHash,randomUUID} from 'node:crypto';
import {dirname,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
// Pure reversible fixture edits only. Never call parts(), material(), or compose().
import {changedImport,reverseImport,changedDecide,reverseDecide} from '../SOURCE_PRESENTATION_GENUINE_VARIANTS_V1_20261005/genuine-presentation-variants.mjs';

export const ROOT_EXECUTION_GATE=null;
export const HERE=dirname(fileURLToPath(import.meta.url)),E=resolve(HERE,'..');
export const W='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/banking-pay-reset-implementation-20261003';
export const sha=x=>createHash('sha256').update(x).digest('hex').toUpperCase();
const lf=x=>x.replace(/\r\n/g,'\n');
const UUID=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const q=x=>"'"+String(x).replaceAll("'","''")+"'",j=x=>q(JSON.stringify(x))+'::jsonb';
export const IDS=Object.freeze({actor:'b8570000-0000-4000-8000-000000000001',candidate:'b8570000-0000-4000-8000-000000000003',contract:'b8570000-0000-4000-8000-000000000014'});
export const OWNER_MATERIAL=Object.freeze({sha256:'B2A9A14155A537F8B1D8C99E718285A8A2F5CB320F798799B0836B750D8CFD66',units:53,finalFunctions:186,creates:199});
export const INPUTS=Object.freeze({
 'SOURCE_PRESENTATION_CAPTURE_CANDIDATE_V1_20261005/GENUINE_PRESENTATION_PREFIX.sql':'A4263FC66915A4A00872B3CE2FBA6A74705CE10CB82399EB6EB009AA0C01BF7A',
 'SOURCE_PRESENTATION_GENUINE_VARIANTS_V1_20261005/genuine-presentation-variants.mjs':'7A3FEA0AF0725B703EA3859BC53ACB65A93E5995B8A18E44BA91513A6AB5F4E2',
 'SOURCE_PRESENTATION_CAPTURE_CANDIDATE_V1_20261005/genuine-presentation-prefix.mjs':'B71D25FD788A49B3D8AF7279348F94FD8B54DF777A599EB3694160AC1B38E472',
 'SOURCE_PRESENTATION_CAPTURE_CANDIDATE_V1_20261005/compose-capture-compatibility.mjs':'70C7E78BB3CB35CF244DE5C65E10809B21031BA91ED42FB4FB51DB80AD8ACD77',
 'MIXED300_FACTUAL_PRODUCER_V1_20261006/run-mixed300-smoke-v1-7.mjs':'CD9DFB3A2DC93BCDE3245C29C23AA1C4468C036D4AFB17994F7FCD4A706E3946',
 'BPAY_NEXT_LOCAL_PAID_RECOVERY_C2_TAIL_V3_20261006.mjs':'3B563836363F642D06476DCE66BC227AAFEDF7C8566DD6C99AC92C8ABF8898E4'
});
// These are current saved dependencies, not the stale presentation composer pins.
// Root's qualified full current catalogue/Source baseline remains the execution authority.
export const CURRENT_SOURCE_PINS=Object.freeze({
 '03092026_1641_contract_settings_effective_authority_v1.sql':'1B0409FD7196096B2A94FF2312AD639A95C9F7770FFD04FE3C58C308564B2E18',
 '03102026_1560_banking_pay_next_source_first_owner_hook_v1.sql':'B1BD8D9BB78E805EC564EE1BF7C328894C9F528700B86A7DA277DBD998A46021',
 '03102026_1548_banking_pay_next_source_chosen_detail_v1.sql':'C8B8C83D183EB392D7339F8D9CCE92A8ADADC401D8469FA74082AD1E9921A819',
 '04102026_2253_weekly_source_candidate_initial_approval_v2.sql':'C4E4121D526F29BDA9B97EE506D3C268687947F5E19BC2DBE22E2412C49036F2',
 '04102026_2307_weekly_source_retain_approved_additional_v2.sql':'D9E2B84C073A8A5279CEE8509D4507BD8D26D8E43BAEDC9848782DEAA43BAC3C',
 '04102026_2317_weekly_source_candidate_head_hours_v2.sql':'C5B59DDB4FE5F1C68C1EA4CA3AE7B6C247B6D2CF205D8DBC8A945D0E17179E39',
 '15092026_1534_00_weekly_source_private_classifiers_v1.sql':'116B4CAB99EDDF64663A84865F40BBAD18FA486101A93E02D14D23A84CD62BF0',
 '15092026_1534_weekly_source_finalisation_v1.sql':'19018EA7BE5E06A510FDD748409BCD84A18D0E31724DAA19AFB0A4471349A95B',
 '15092026_1534_weekly_source_projection_build_v1.sql':'6B6F88E3063963738741BF0B0245F7069A4A558BE0E32DB96A4FBEFD0A00F107',
 '15092026_1534_weekly_source_ordinary_pay_projection_v1.sql':'665DDD27108A867F92E1F04DD346130736DF27031A577F108B7F9C1BF6807900',
 '15092026_1534_weekly_source_protected_action_orchestration_v1.sql':'2E3DA9EDDDCE5DE7815CFEAA50A4257C345957523ECBA2783E7F7697BFB28B75',
 '15092026_1534_weekly_source_read_projections_v1.sql':'DACE15A8DF0B37DECF261BEB3CD99AC5C59EA362F22114B382C1C0148D4BF0E4',
 '15092026_1534_weekly_source_upload_publication_v1.sql':'A06BD86711D35D29B807D97E54C2BB81303CCBC37B86832CC624E88E5867EB0B',
 '17092026_0300_weekly_source_entitlement_publication_v1.sql':'FA4676D00888C5BC400F2DF86EEB4C2FCD6BB159462522D16836FAEFA1108ADE',
 '17092026_0600_weekly_source_first_authorisation_v1.sql':'09FBE853B875393170EA2F538EDDD41A0E4BE41AB65CF5CB54CB663EAE2ADA71',
 '17092026_1100_weekly_source_candidate_view_producer_v1.sql':'077E1F34271ADFB97EE6D5D3D2D718CE21186F0C1A9D99E221C51D94DD02B77A',
 '17092026_1200_weekly_source_audit_and_export_v1.sql':'8AFB340A7222B9D66469758A6C56FB7C7DE7A004C3FE54F6183C86532B571DCD'
});
const exactOne=(s,a,b)=>{assert.equal(s.split(a).length,2,'URZ_EXACT_ONE_FIXTURE_HUNK');return s.replace(a,b);};
const NS=Object.freeze([
 ['b8550000-','b8570000-'],['pg_temp.bpsc_','pg_temp.urz_'],['pg_temp.bpspv_decide','pg_temp.urz_decide'],
 ['bpay-next-import-owner@example.test','unpaid-zero-owner@example.test'],
 ['NEXT imported Source worker','UNPAID ZERO Source worker'],['NEXT imported NHSP Trust','UNPAID ZERO NHSP Trust'],
 ['BPAY_NEXT_IMPORT_ROSTER','URZ_IMPORT_ROSTER'],['BPAY_NEXT_IMPORT_NHSP','URZ_IMPORT_NHSP']
]);
const scope=s=>NS.reduce((x,[a,b])=>x.split(a).join(b),s);
const unscope=s=>[...NS].reverse().reduce((x,[a,b])=>x.split(b).join(a),s);
const CANDIDATE_BEFORE="insert into public.candidates(id,display_name)\nvalues('b8550000-0000-4000-8000-000000000003','NEXT imported Source worker');";
const CANDIDATE_AFTER="insert into public.candidates(id,display_name,active,pay_method)\nvalues('b8550000-0000-4000-8000-000000000003','NEXT imported Source worker',true,'PAYE');";
function helper(raw,name){
 const start=raw.indexOf('create function pg_temp.'+name+'(');assert(start>=0,'URZ_SAVED_HELPER_MISSING:'+name);
 assert.equal(raw.split('create function pg_temp.'+name+'(').length,2);
 const end=raw.indexOf('$f$;',start);assert(end>start);return raw.slice(start,end+5);
}
function sliceFunction(raw,start,end){
 assert.equal(raw.split(start).length,2);assert.equal(raw.split(end).length,2);
 const a=raw.indexOf(start),b=raw.indexOf(end,a);assert(b>a);return raw.slice(a,b).trim().replace(/^export /,'');
}
// Pure shape/inverse/provenance checks. No native module is imported or executed.
export function sourceControls(){
 for(const [file,pin] of Object.entries(INPUTS))assert.equal(sha(readFileSync(resolve(E,file))),pin,'URZ_INPUT_DRIFT:'+file);
 const dependencies=Object.entries(CURRENT_SOURCE_PINS).map(([file,pin])=>{
  const raw=readFileSync(resolve(W,'supabase/repeatable',file));assert.equal(sha(raw),pin,'URZ_CURRENT_SOURCE_DRIFT:'+file);return lf(raw.toString());
 }).join('\n');
 const raw=lf(readFileSync(resolve(E,Object.keys(INPUTS)[0]),'utf8'));
 const names=['bpsc_assert','bpsc_import','bpsc_service','bpsc_project_prior','bpspv_decide'];
 const original=names.map(name=>helper(raw,name));
 const changed=original.map((s,i)=>i===1?changedImport(s):i===4?changedDecide(s):s);
 assert.equal(reverseImport(changed[1]),original[1],'URZ_EXACT_IMPORT_INVERSE');
 assert.equal(reverseDecide(changed[4]),original[4],'URZ_EXACT_ZERO_VECTOR_INVERSE');
 const a=raw.indexOf('insert into public.settings_defaults('),b=raw.indexOf('create function pg_temp.bpsc_import(');
 assert(a>0&&b>a);const oldFacts=raw.slice(a,b).trim();
 const facts=exactOne(oldFacts,CANDIDATE_BEFORE,CANDIDATE_AFTER);
 assert.equal(exactOne(facts,CANDIDATE_AFTER,CANDIDATE_BEFORE),oldFacts,'URZ_CANDIDATE_FACT_INVERSE');
 const unscoped=[facts,...changed].join('\n\n'),sql=scope(unscoped);
 assert.equal(unscope(sql),unscoped,'URZ_COMPLETE_NAMESPACE_INVERSE');
 assert.equal((sql.match(/create function pg_temp\./g)??[]).length,5,'URZ_ONLY_FIVE_TEMP_HELPERS');
 assert(!/create (?:or replace )?function (?:public|private)\.|(?:insert into|update|delete from) private\.|alter |grant |set role|set constraints/i.test(sql),'URZ_NO_OWNER_OR_FINANCIAL_AUTHORITY_WRITES');
 assert(!/pg_catalog\.(?:coalesce|nullif|least|greatest)\(/i.test(sql),'URZ_NO_SCHEMA_QUALIFIED_SQL_SPECIAL_FORMS');
 const publicCalls=['weekly_source_upload_stage_begin_atomic_v1','weekly_source_upload_stage_rows_atomic_v1',
  'weekly_source_upload_seal_atomic_v1','weekly_source_projection_begin_atomic_v1','weekly_source_projection_rows_apply_atomic_v1',
  'weekly_source_projection_publish_atomic_v1','weekly_source_finalise_atomic_v1','weekly_source_ordinary_pay_projection_apply_atomic_v1',
  'weekly_source_first_authorise_v1','weekly_source_later_change_decide_atomic_v1'].map(x=>'public.'+x);
 for(const name of publicCalls)assert(new RegExp('create (?:or replace )?function '+name.replace('.','\\.')+'\\(', 'i').test(dependencies),'URZ_CURRENT_PUBLIC_SOURCE_DEPENDENCY:'+name);
 const bridge=lf(readFileSync(resolve(E,'MIXED300_FACTUAL_PRODUCER_V1_20261006/run-mixed300-smoke-v1-7.mjs'),'utf8'));
 const callers=lf(readFileSync(resolve(E,'BPAY_NEXT_LOCAL_PAID_RECOVERY_C2_TAIL_V3_20261006.mjs'),'utf8'));
 return {sql,helperPins:names.map((name,i)=>({name,preimageSha256:sha(original[i]),changedSha256:sha(changed[i])})),
  facts:{preimageSha256:sha(oldFacts),changedSha256:sha(facts)},fixtureSqlSha256:sha(sql),publicCalls,
  unchangedFunctions:{localBridge:sliceFunction(bridge,'export async function localBridge(c,p){','\n// Closed observation only. No additional RPC, query, retry or result rewrite.'),
   createC2SavedCallers:sliceFunction(callers,'export function createC2SavedCallers(c,actor,witness=null){','\nconst exactMoney='),
   createC2FailureWitness:sliceFunction(callers,'export function createC2FailureWitness(ownerSources=[]){','\nexport function createC2SavedCallers(')}};
}

export function constraintModes(constraints){
 assert(Array.isArray(constraints));
 for(const x of constraints)assert(x&&typeof x.deferrable==='boolean'&&typeof x.deferred==='boolean'&&(!x.deferred||x.deferrable));
 const defs=constraints.filter(x=>x.deferrable),later=defs.filter(x=>x.deferred),now=defs.filter(x=>!x.deferred);
 assert.equal(defs.length,35);assert.equal(later.length,28);assert.equal(now.length,7);
 for(const x of defs){assert(['public','private'].includes(x.schema)&&/^[a-z_][a-z0-9_]{0,62}$/.test(x.name)&&/^[a-z_][a-z0-9_]{0,62}$/.test(x.relation)&&['f','t'].includes(x.type)&&x.validated===true);
  assert.equal(constraints.filter(y=>y.schema===x.schema&&y.name===x.name).length,1);}
 assert.equal(sha(JSON.stringify(defs)),'75253E3CD104BD9D463BDAD57DB058982F06958B3302EB945959CBA14B7EFD10');
 assert.equal(sha(JSON.stringify(later)),'27959973B9E267B854D03C78B3FC0E8992704BB56CD27CC1C7DC423D6E63EDBE');
 assert.equal(sha(JSON.stringify(now)),'30334E5511B89F879F357F9AD0037D3B2CC539EB4E2193AF9BECF56451B8F0D7');
 return 'set constraints '+later.map(x=>JSON.stringify(x.schema)+'.'+JSON.stringify(x.name)).join(',')+' deferred;';
}
const importedRows=Object.freeze({
 initial:[{key:'URZ-INITIAL-1',date:'2026-09-01',end:'19:00',minutes:570,break:30,expense:0,sign:1},{key:'URZ-INITIAL-2',date:'2026-09-02',end:'17:00',minutes:420,break:60,expense:0,sign:1}],
 positive:[{key:'URZ-INDEPENDENT-P4',date:'2026-09-21',end:'13:00',minutes:240,break:0,expense:0,sign:1}]
});
const sourceWork=root=>`(select jsonb_build_object('work',w.id,'revision',w.current_revision_id,'command',p.command_id,'amount',r.approved_source_ex_vat::text,'certified_zero',r.certified_zero,'line_count',r.expected_line_count,'ready',w.current_revision_id=w.applied_revision_id)
 from private.bpay_next_work w join private.bpay_next_work_revision r on r.id=w.current_revision_id join private.bpay_next_publication p on p.work_id=w.id and p.revision_id=r.id where w.original_timesheet_id=${q(root)}::uuid)`;
const frozenRoot=root=>`(select to_jsonb(f) from public.timesheets_financials f where f.timesheet_id=${q(root)}::uuid and f.is_current)`;
function pointSql(roots){
 roots.forEach(x=>assert.match(x,UUID));const list=roots.map(x=>q(x)+'::uuid').join(',');
 return `(select jsonb_build_object(
 'financials',(select coalesce(jsonb_agg(to_jsonb(f) order by f.id),'[]') from public.timesheets_financials f where f.timesheet_id in (${list})),
 'authorisations',(select coalesce(jsonb_agg(to_jsonb(a) order by a.root_timesheet_id),'[]') from public.weekly_source_root_authorisations a where a.root_timesheet_id in (${list})),
 'heads',(select coalesce(jsonb_agg(to_jsonb(h) order by h.id),'[]') from public.weekly_source_entitlement_heads h where h.root_timesheet_id in (${list})),
 'works',(select coalesce(jsonb_agg(to_jsonb(w) order by w.id),'[]') from private.bpay_next_work w where w.original_timesheet_id in (${list})),
 'positions',(select coalesce(jsonb_agg(to_jsonb(p) order by p.work_id,p.component_key),'[]') from private.bpay_next_position p join private.bpay_next_work w on w.id=p.work_id where w.original_timesheet_id in (${list})),
 'bindings',(select coalesce(jsonb_agg(to_jsonb(v) order by v.id),'[]') from private.bpay_next_source_component_binding b join private.bpay_next_source_component_binding_version v on v.id=b.current_binding_version_id where b.candidate_id=${q(IDS.candidate)}::uuid),
 'collections',(select coalesce(jsonb_agg(to_jsonb(c) order by c.id),'[]') from private.bpay_next_work_collection c where c.candidate_id=${q(IDS.candidate)}::uuid),
 'cases',(select coalesce(jsonb_agg(to_jsonb(c) order by c.id),'[]') from private.bpay_next_finance_case c where c.candidate_id=${q(IDS.candidate)}::uuid)))`;
}
const namespaceSql=`(select jsonb_build_object('candidate',exists(select 1 from public.candidates where id=${q(IDS.candidate)}::uuid),'actor',exists(select 1 from public.tms_users where id=${q(IDS.actor)}::uuid),'contracts',(select count(*) from public.contracts where id in ('b8570000-0000-4000-8000-000000000004','b8570000-0000-4000-8000-000000000014')),'clients',(select count(*) from public.clients where id in ('b8570000-0000-4000-8000-000000000002','b8570000-0000-4000-8000-000000000012')),'groups',(select count(*) from public.weekly_source_groups where id in ('b8570000-0000-4000-8000-000000000005','b8570000-0000-4000-8000-000000000015'))))`;
const noNamespace={candidate:false,actor:false,contracts:0,clients:0,groups:0};
const closedFailure=(error,witness)=>({sqlstate:/^[0-9A-Z]{5}$/.test(error?.sqlstate??'')?error.sqlstate:null,
 assertion:error?.code==='ERR_ASSERTION'?String(error.message).split('\n')[0].replace(/[^A-Za-z0-9_: .-]/g,'').slice(0,160):null,
 savedCaller:witness?.snapshot(error)??null});

// Only root may invoke this after source/independent review, on its existing BEGIN
// connection. Root supplies unchanged already-loaded functions; no dynamic imports.
export async function runUnpaidZeroPublicReproduction({connection,localBridge,createC2SavedCallers,createC2FailureWitness,
 ownerSql,constraintDefaults,rootGate=ROOT_EXECUTION_GATE}={}){
 assert(rootGate&&rootGate.scope==='SOURCE_UNPAID_ZERO_PUBLIC_REPRO_V1'&&rootGate.rollbackOnly===true&&rootGate.freshVerificationOwnedByRoot===true,'URZ_HELD_ROOT_GATE');
 assert.equal(rootGate.adapterSha256,sha(readFileSync(fileURLToPath(import.meta.url))),'URZ_REVIEWED_ADAPTER');
 for(const k of ['rootReviewSha256','independentReviewSha256'])assert.match(rootGate[k]??'',/^[A-F0-9]{64}$/,'URZ_REVIEW_HASH_REQUIRED');
 assert.equal(rootGate.currentOwnerCatalogueQualified,true);assert.equal(rootGate.currentSourceBaselineQualified,true);assert.equal(rootGate.serviceSecurityQualified,true);
 assert.equal(sha(ownerSql),OWNER_MATERIAL.sha256,'URZ_CURRENT_COMPOUND_GUARD_MATERIAL');
 assert.deepEqual(rootGate.materialCounts,{units:53,finalFunctions:186,creates:199});
 const controls=sourceControls(),restore=constraintModes(constraintDefaults);
 for(const [name,fn] of Object.entries({localBridge,createC2SavedCallers,createC2FailureWitness})){
  assert.equal(typeof fn,'function','URZ_SHARED_FUNCTION_REQUIRED:'+name);
  assert.equal(lf(fn.toString()).trim(),controls.unchangedFunctions[name],'URZ_UNCHANGED_SHARED_FUNCTION:'+name);
 }
 const bridge=await localBridge(connection,{actor:IDS.actor,candidates:[{id:IDS.candidate}],contracts:[{id:IDS.contract}],slots:[],counts:{selected_work:2}});
 const c=bridge.connection;assert.equal(typeof c.drain,'function');
 // Observations for CURRENT_WORK/COMPONENTS use the same saved callers without
 // a witness, because the existing closed witness does not admit those names.
 const plain=createC2SavedCallers(c,IDS.actor);
 const answer={schema_version:'SOURCE_UNPAID_ZERO_PUBLIC_REPRO_DIAGNOSTIC_V1',status:'NOT_EXECUTED',fixtureSqlSha256:controls.fixtureSqlSha256,
  prediction:{positiveOnly:'Source-only expectation: independent positive work can reach real PREPARE/REVIEW without a Case.',
   zeroPlusPositive:'Source-only expectation: selected unpaid REMOVED with no approved line may cause FINANCIAL_RESOLUTION_REQUIRED; legitimate no-admission/refusal is an observation, not a failed success assertion.'},
  actual:{sourceStages:[],baseline:null,pages:null,attempts:[]},rootFreshGate:null,
  awards:{generated01:false,removedComparison:false,adversaries:false,payment:false,hosted:false},cleanup:null};
 let phase='PREFLIGHT',scopeOpen=false;
 try{
  assert.equal(await c.query('current_database()'),'cloudtms_test_clone','URZ_TEST_DATABASE');
  assert.equal(await c.query("exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT')"),true,'URZ_EXISTING_NEXT_OWNER_REQUIRED');
  assert.deepEqual(await c.query(namespaceSql),noNamespace,'URZ_EXACT_NAMESPACE_ABSENT');
  assert.equal(await c.query("exists(select 1 from private.bpay_next_command where status in ('QUEUED','ENROLLING','SEALED'))"),false,'URZ_NO_FOREIGN_PENDING_ENROLLMENT');
  await c.sql('set constraints all immediate;');await c.sql(restore);
  await c.sql('savepoint urz_fixture;');scopeOpen=true;phase='SOURCE_FACTS_AND_TEMP_HELPERS';await c.sql(controls.sql);
  const importRows=async(week,rows,key)=>c.query(`pg_temp.urz_import(true,${q(week)}::date,${j(rows)},${q(key)})`);
  const events=async(upload,keys)=>{
   assert.match(upload,UUID);assert(keys.length<=2);
   const rows=await c.query(`(select coalesce(jsonb_agg(jsonb_build_object('key',u.external_source_key,'root',l.timesheet_id,'event',r.work_event_id) order by u.external_source_key),'[]') from public.weekly_source_upload_rows u join public.weekly_source_row_resolutions r on r.upload_row_id=u.id join public.weekly_source_row_timesheet_lineages l on l.row_resolution_id=r.id where u.upload_id=${q(upload)}::uuid and u.external_source_key in (${keys.map(q).join(',')}))`);
   assert.equal(rows.length,keys.length,'URZ_EXACT_REAL_LINEAGE');rows.forEach(x=>{assert.match(x.root,UUID);assert.match(x.event,UUID);});return rows;
  };
  const project=async(final,root,key)=>{assert.match(final,UUID);assert.match(root,UUID);await c.query(`pg_temp.urz_project_prior(${q(final)}::uuid,${q(root)}::uuid,${q(key)})`);};
  const settle=async(root,label)=>{
   const w=await c.query(sourceWork(root));assert(w,'URZ_ACTUAL_SOURCE_WORK_PUBLICATION');for(const k of ['work','revision','command'])assert.match(w[k],UUID);
   const witness=createC2FailureWitness([ownerSql]),api=createC2SavedCallers(c,IDS.actor,witness);
   try{await api.enrollCommand(w.command,'POSITION_APPLY');}catch(error){answer.actual.sourceStages.push({label,failure:closedFailure(error,witness)});throw error;}
   const current=await c.query(sourceWork(root));assert.equal(current.ready,true,'URZ_REAL_POSITION_APPLY_TERMINAL');answer.actual.sourceStages.push({label,...current});return current;
  };
  phase='SOURCE_INITIAL';const initial=await importRows('2026-09-06',importedRows.initial,'URZ-GENUINE-INITIAL-TWO');
  const first=await events(initial.upload_id,importedRows.initial.map(x=>x.key));const zRoot=first[0].root;
  assert.equal(first[1].root,zRoot);assert.notEqual(first[0].event,first[1].event);
  await project(initial.final_revision_id,zRoot,'URZ-FIRST-PROJECTION');
  const firstReply=await c.query(`public.weekly_source_first_authorise_v1(${q(zRoot)}::uuid,${q(zRoot)}::uuid,null,${q(IDS.actor)}::uuid)`);
  assert.equal(firstReply.ok,true,'URZ_REAL_FIRST_AUTHORISATION');await settle(zRoot,'initial');
  const originalFinancial=await c.query(frozenRoot(zRoot));assert(originalFinancial);
  const laterRows=[
   {key:'URZ-N9H5',date:'2026-09-01',end:'19:00',minutes:570,break:30,expense:0,sign:-1,prior_event:first[0].event},
   {key:'URZ-P8',date:'2026-09-01',end:'17:30',minutes:480,break:30,expense:0,sign:1,prior_event:first[0].event},
   {key:'URZ-N7',date:'2026-09-02',end:'17:00',minutes:420,break:60,expense:0,sign:-1,prior_event:first[1].event},
   {key:'URZ-P5H5',date:'2026-09-02',end:'15:30',minutes:330,break:60,expense:0,sign:1,prior_event:first[1].event}];
  phase='SOURCE_TWO_COMPONENT_APP';const later=await importRows('2026-09-13',laterRows,'URZ-GENUINE-REPLACEMENT');
  await project(later.final_revision_id,zRoot,'URZ-PROJECTION-13H5');
  await c.query(`pg_temp.urz_decide(${q(zRoot)}::uuid,${q(later.final_revision_id)}::uuid,'APPROVE_UPDATED_HOURS','URZ-APP-13H5')`);await settle(zRoot,'replacement');
  const zeroRows=[{key:'URZ-ZERO-N8',date:'2026-09-01',end:'17:30',minutes:480,break:30,expense:0,sign:-1,prior_event:first[0].event},
   {key:'URZ-ZERO-N5H5',date:'2026-09-02',end:'15:30',minutes:330,break:60,expense:0,sign:-1,prior_event:first[1].event}];
  phase='SOURCE_CERTIFIED_ZERO';const zero=await importRows('2026-09-20',zeroRows,'URZ-GENUINE-FULL-NEGATIVE-ZERO');
  assert.deepEqual(await c.query(`private.weekly_source_ordinary_projection_current_segments_v1(${q(zRoot)}::uuid,${q(zero.final_revision_id)}::uuid)`),[],'URZ_REAL_EMPTY_FINAL_VECTOR');
  await project(zero.final_revision_id,zRoot,'URZ-PROJECTION-ZERO');
  const zeroReply=await c.query(`pg_temp.urz_decide(${q(zRoot)}::uuid,${q(zero.final_revision_id)}::uuid,'APPROVE_UPDATED_HOURS','URZ-APP-CERTIFIED-ZERO')`);
  const zWork=await settle(zRoot,'certifiedZero');
  const head=await c.query(`private.weekly_source_candidate_head_hours_v2(${q(zRoot)}::uuid)`);
  assert.equal(head.state,'AVAILABLE');assert.equal(head.certified_zero,true);assert.equal(Number(head.total_hours),0);assert.deepEqual(head.rows,[]);assert.deepEqual(zeroReply.component_ids,[]);
  assert.equal(zWork.certified_zero,true);assert.equal(Number(zWork.amount),0);assert.equal(zWork.line_count,0);
  assert.deepEqual(await c.query(frozenRoot(zRoot)),originalFinancial,'URZ_FROZEN_CURRENT_TSFIN_UNTOUCHED');
  phase='INDEPENDENT_POSITIVE_SAME_CANDIDATE_CONTRACT';const positive=await importRows('2026-09-27',importedRows.positive,'URZ-INDEPENDENT-FIRST');
  const pRoot=(await events(positive.upload_id,['URZ-INDEPENDENT-P4']))[0].root;assert.notEqual(pRoot,zRoot,'URZ_DISTINCT_REAL_ROOTS');
  await project(positive.final_revision_id,pRoot,'URZ-POSITIVE-FIRST-PROJECTION');
  assert.equal((await c.query(`public.weekly_source_first_authorise_v1(${q(pRoot)}::uuid,${q(pRoot)}::uuid,null,${q(IDS.actor)}::uuid)`)).ok,true);
  const pWork=await settle(pRoot,'independentPositive');assert.equal(Number(pWork.amount),80);assert.equal(pWork.certified_zero,false);
  assert.equal(await c.query(`(select count(*)=2 and bool_and(t.contract_id=${q(IDS.contract)}::uuid and ct.candidate_id=${q(IDS.candidate)}::uuid) and count(distinct t.week_ending_date)=2 from public.timesheets t join public.contracts ct on ct.id=t.contract_id where t.timesheet_id in (${q(zRoot)}::uuid,${q(pRoot)}::uuid))`),true,'URZ_REAL_SAME_CANDIDATE_CONTRACT_DISTINCT_WEEKS');
  await c.sql('set constraints all immediate;');await c.sql(restore);
  const roots=[zRoot,pRoot],baseline=await c.query(pointSql(roots));
  assert.equal(baseline.cases.length,0);assert.equal(baseline.collections.length,0,'URZ_UNPAID_NO_SEEDED_COLLECTION');
  assert(baseline.bindings.length>0&&baseline.bindings.length<=8,'URZ_FINITE_ACTUAL_SOURCE_BINDINGS');
  const removed=baseline.bindings.filter(b=>b.approval_work_id===zWork.work);assert(removed.length>0,'URZ_ACTUAL_ZERO_REMOVED_BINDINGS');
  for(const b of removed){assert.equal(b.component_state,'REMOVED');assert.equal(b.approved_line_id,null);assert.equal(b.approval_revision_id,zWork.revision);assert.equal(b.approval_source_channel,'PAYE');}
  for(const p of baseline.positions){assert.equal(Number(p.realised_source_ex_vat),0);assert.equal(Number(p.held_source_ex_vat),0);}
  const zeroPositions=baseline.positions.filter(p=>p.work_id===zWork.work);assert(zeroPositions.length>0&&zeroPositions.length<=8,'URZ_FINITE_ACTUAL_ZERO_POSITIONS');
  for(const p of zeroPositions){assert.equal(Number(p.approved_source_ex_vat),0);assert.equal(Number(p.approved_source_ex_vat)-Number(p.realised_source_ex_vat)-Number(p.held_source_ex_vat),0,'URZ_ACTUAL_UNPAID_ZERO_RESIDUAL');}
  answer.actual.baseline={roots,zeroWork:zWork,positiveWork:pWork,head,pointSha256:sha(JSON.stringify(baseline)),frozenOriginalSha256:sha(JSON.stringify(originalFinancial))};
  phase='PUBLIC_VISIBILITY';answer.actual.pages={currentWork:await plain.page('CURRENT_WORK',{candidate_id:IDS.candidate,limit:'100'}),
   zeroComponents:await plain.page('COMPONENTS',{work_id:zWork.work,expected_revision_id:zWork.revision,limit:'100'}),
   positiveComponents:await plain.page('COMPONENTS',{work_id:pWork.work,expected_revision_id:pWork.revision,limit:'100'})};
  for(const [name,works] of [['positiveOnly',[pWork]],['zeroPlusPositive',[zWork,pWork]]]){
   phase='ATTEMPT_'+name;const record={name,selected:works.map(w=>w.work),stage:'NOT_STARTED',actual:null,failure:null,rollback:null};
   const witness=createC2FailureWitness([ownerSql]),api=createC2SavedCallers(c,IDS.actor,witness);
   const run=randomUUID(),command=randomUUID();await c.sql('savepoint urz_attempt;');
   try{
    record.stage='CREATE';record.created=await api.preparation('CREATE',{run_id:run,pay_date:'2026-10-09'});
    const args={run_id:run,page_no:'1',choices:works.map(w=>({work_id:w.work,expected_revision_id:w.revision,mode:'ALL'}))};
    record.stage='WORK_PAGE';record.workPage=await api.preparation('WORK_PAGE',args);record.workPageReplay=await api.preparation('WORK_PAGE',args);assert.equal(record.workPageReplay.replay,true);
    record.stage='SEAL_ENQUEUE';record.sealed=await api.preparation('SEAL_ENQUEUE',{run_id:run,command_id:command,expected_pages:'1',expected_count:String(works.length)});
    record.stage='PREPARE';record.jobId=await api.enrollCommand(command,'PREPARE');
    record.stage='REVIEW';record.review=await api.preparation('REVIEW',{run_id:run});
    record.actual=await c.query(`(select jsonb_build_object('workers',(select coalesce(jsonb_agg(to_jsonb(w) order by w.id),'[]') from private.bpay_next_run_worker w where w.run_id=${q(run)}::uuid),'positions',(select coalesce(jsonb_agg(to_jsonb(p) order by p.id),'[]') from private.bpay_next_run_position p join private.bpay_next_run_worker w on w.id=p.run_worker_id where w.run_id=${q(run)}::uuid),'job',(select to_jsonb(j) from private.bpay_next_job j where j.id=${q(record.jobId)}::uuid)))`);
    record.workersPage=await plain.page('WORKERS',{run_id:run,limit:'100'});record.stage='STOPPED_AFTER_REAL_REVIEW';
    await c.sql('set constraints all immediate;');await c.sql(restore);record.constraintValidation='ACTUAL_ALL_IMMEDIATE_ACCEPTED';
   }catch(error){record.failure=closedFailure(error,witness);record.actualUnavailableAfterFailure=record.actual===null;}
   finally{
    await c.drain();await c.sql('rollback to savepoint urz_attempt;');await c.sql('release savepoint urz_attempt;');await c.sql(restore);
    assert.deepEqual(await c.query(pointSql(roots)),baseline,'URZ_EXACT_SAME_BASELINE_AFTER_EACH_ATTEMPT');
    record.rollback={savepoint:true,sameBaseline:true,defaultDeferredRestored:28,originalImmediateRetained:7};answer.actual.attempts.push(record);
   }
  }
  answer.status='DIAGNOSTIC_OBSERVED_NO_ACCEPTANCE_AWARD';
 }catch(error){answer.status='STOP_SOURCE_PREREQUISITE_OR_OBSERVATION';answer.actual.stop={phase,failure:closedFailure(error,null)};}
 finally{
  if(scopeOpen){await c.drain();await c.sql('rollback to savepoint urz_fixture;');await c.sql('release savepoint urz_fixture;');await c.sql(restore);
   assert.deepEqual(await c.query(namespaceSql),noNamespace,'URZ_FACT_NAMESPACE_REMOVED_BY_SAVEPOINT');
   answer.cleanup={fixtureSavepointRolledBack:true,namespaceAbsent:true,outerRollback:'ROOT_REQUIRED',freshConnectionEquality:'ROOT_REQUIRED'};}
 }
 return answer;
}
