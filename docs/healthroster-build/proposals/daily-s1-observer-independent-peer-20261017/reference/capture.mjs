// Source authoring only. Never SQL execution; no Banking semantic body selection.
import fs from 'node:fs';import path from 'node:path';import{execFileSync}from'node:child_process';import{createHash}from'node:crypto';import{fileURLToPath,pathToFileURL}from'node:url';import assert from'node:assert/strict';
const here=path.dirname(fileURLToPath(import.meta.url)),root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',sha=b=>createHash('sha256').update(b).digest('hex'),git=(...a)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,...a],{maxBuffer:16*1024*1024});
const {scanManagedSql}=await import(pathToFileURL(root+'/scripts/cloudtms-managed-release-sql.mjs'));
const {routineDefinitions,definerInventory}=await import(pathToFileURL(root+'/scripts/bpay-next-final-definers.mjs'));
const selectors=[
 ['public.client_create_with_settings_v1','supabase/repeatable/22092026_1226_client_initial_settings_baseline_v1.sql'],
 ['public._change_bump','supabase/baseline/22082026_1503_cloudtms_test_routines_00.sql'],
 ['public._trg_change_bump','supabase/baseline/22082026_1503_cloudtms_test_routines_03.sql'],
 ['public._trg_touch_client_from_client_settings','supabase/baseline/22082026_1503_cloudtms_test_routines_03.sql'],
 ['public.trg_set_cli_ref','supabase/baseline/22082026_1503_cloudtms_test_routines_19.sql'],
 ['public.trg_set_rev','supabase/baseline/22082026_1503_cloudtms_test_routines_19.sql'],
 ['public.set_updated_at','supabase/baseline/22082026_1503_cloudtms_test_routines_17.sql'],
 ['private._invoice_candidate_revision_trigger_v2','supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1042_05_private_invoice_candidate_revision_trigger_v2.sql'],
 ['private._invoice_candidate_snapshot_bump_v2','supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1042_04_private_invoice_candidate_snapshot_bump_v2.sql'],
 ['private._invoice_jsonb_pick_v2','supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1042_05_private_invoice_candidate_revision_trigger_v2.sql'],
 ['private._contract_settings_after_client_v1','supabase/repeatable/03092026_1641_contract_settings_effective_authority_v1.sql'],
 ['private._contract_settings_refresh_planned_for_client_v1','supabase/repeatable/04092026_1901_client_planned_override_refresh_v1.sql'],
 ['public._inv_write_audit','supabase/repeatable/08012026_invoice_sql_first_rpcs_generator_paid_batches.sql'],
 ['public._ctms_assert_import_correction_settings_write_v1','supabase/repeatable/21072026_1820_00b_import_correction_settings_guard.sql'],
 ['private._candidate_feature_enabled_current_v1','supabase/repeatable/07082026_2059_candidate_app_private_helpers_v1.sql']
];
const contractPath='supabase/release/current-contract.json',contract=JSON.parse(fs.readFileSync(root+'/'+contractPath));
const relations=['clients','client_settings','audit_events','contracts','contract_weeks','app_change_counters','banking_pay_scope_change_transactions'];
const sequences=['public.client_cliref_seq','public.global_rev_seq','private.invoice_generate_candidate_change_seq','private.invoice_issue_candidate_change_seq'];
const rows=contract.relations.filter(r=>r.schema==='public'&&relations.includes(r.name)||sequences.includes(r.schema+'.'+r.name));assert.equal(rows.length,11);
const triggerRows=contract.triggers.filter(t=>t.schema==='public'&&relations.includes(t.table));assert(triggerRows.length>0);
// This is catalogue metadata. No routine definition/body text is present.
const wantedNames=new Set(selectors.map(s=>s[0].split('.').at(-1)).concat(['pay_workbench_mark_contract_client_dirty','pay_workbench_scope_counter_stage_trg_v1','pay_workbench_scope_change_finalize_trg_v1','_contract_settings_effective_core_v1','import_auto_authorise_policy_resolve_v1','_candidate_normalize_manager_policy_v1','_candidate_manager_authoriser_policy_normalize_v2']));
const routines=contract.routines.filter(r=>wantedNames.has(r.identity.split('(')[0].split('.').at(-1)));
const native='docs/healthroster-build/v6-owner-delivery-20261016/GENUINE_CLIENT_NATIVE_SOURCE_IDENTITY_20261017.json',s0='docs/healthroster-build/v6-owner-delivery-20261016/DAILY_S0_FIXED_NATIVE_METADATA_SUMMARY_20261017.json';
const files=[...new Set([...selectors.map(r=>r[1]),contractPath,native,s0])],parents=[];
for(const file of files){const b=fs.readFileSync(root+'/'+file);assert(b.equals(git('show',':'+file))&&b.equals(git('show','HEAD:'+file)));parents.push({file,bytes:b.length,sha256:sha(b)});}
const captures=[];const out=[];
for(const [name,file]of selectors){const source=fs.readFileSync(root+'/'+file,'utf8'),found=[];for(const r of scanManagedSql(source).statements){const text=source.slice(r.start,r.end),defs=routineDefinitions(text,file);if(defs.some(d=>d.startsWith(name+'(')))found.push({text,start:r.start,end:r.end,signature:defs[0]});}assert.equal(found.length,1,name);const row=found[0],rel='reference/'+name.replace('.','__')+'.sql';captures.push({signature:row.signature,file,start:row.start,end:row.end,bytes:Buffer.byteLength(row.text),sha256:sha(row.text),capture:rel});out.push([rel,Buffer.from(row.text)]);}
const selectionFile=here+'/SOURCE_SELECTION.json',old=fs.existsSync(selectionFile)?JSON.parse(fs.readFileSync(selectionFile)):null;
const rootCommit=old?.rootCommit??git('rev-parse','HEAD').toString().trim();for(const p of parents)assert(fs.readFileSync(root+'/'+p.file).equals(git('show',rootCommit+':'+p.file)),p.file);
const ownerInventory=definerInventory();for(const c of captures){const owners=ownerInventory.routines.get(c.signature)??[];if(owners.length){assert.equal(owners.at(-1).physical,c.file,c.signature);c.managedOwner={root:owners.at(-1).root,physical:owners.at(-1).physical,closureSha256:owners.at(-1).closureSha256};}else{assert(c.file.startsWith('supabase/baseline/'),c.signature);c.managedOwner=null;}}
const selection={version:'DAILY_S1_SOURCE_AND_METADATA_V1',root,rootCommit,parents,captures,nativeDefinitionReceipt:native,nativeS0MetadataReceipt:s0,semanticBankBodies:[],runtimeTargets:[]};
out.push(['SOURCE_SELECTION.json',Buffer.from(JSON.stringify(selection,null,2)+'\n')]);out.push(['SOURCE_METADATA.json',Buffer.from(JSON.stringify({version:'SOURCE_CATALOGUE_ONLY_NOT_NATIVE',contractSha256:sha(fs.readFileSync(root+'/'+contractPath)),relations:rows,triggers:triggerRows,routines},null,2)+'\n')]);
if(process.argv.includes('--record')){assert(!fs.existsSync(here+'/MANIFEST.json'),'immutable packet');for(const[f,b]of out){fs.mkdirSync(path.dirname(here+'/'+f),{recursive:true});fs.writeFileSync(here+'/'+f,b);}}else for(const[f,b]of out)assert(b.equals(fs.readFileSync(here+'/'+f)),f);
console.log(JSON.stringify({parents:parents.length,healthGeneralCompleteStatements:captures.length,relationsAndSequences:rows.length,sourceTriggerAttachments:triggerRows.length,bankSemanticBodies:0,runtimeTargets:0,native:'NOT_RUN'}));
