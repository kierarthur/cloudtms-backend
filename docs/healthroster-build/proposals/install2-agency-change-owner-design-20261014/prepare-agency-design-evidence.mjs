import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {pathToFileURL,fileURLToPath} from 'node:url';

const own = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(process.argv[2] || '');
const expected = '74a9c10f7370cd3641ee95d31f6702ecadd900a7';
const git = (...args) => execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],{cwd:root,maxBuffer:96*1024*1024});
const sha = bytes => crypto.createHash('sha256').update(bytes).digest('hex');
if (git('rev-parse','HEAD').toString().trim() !== expected) throw new Error('ACTUAL_CURRENT_SOURCE_PIN_CHANGED');
const compiler = 'scripts/cloudtms-db-release-lib.mjs';
const compilerBytes = fs.readFileSync(path.join(root,compiler));
if (!compilerBytes.equals(git('show',`${expected}:${compiler}`))) throw new Error('COMPILER_SAVED_GIT_MISMATCH');
// Only inventory/closure are called. No connection, psql, executable-file copy,
// SQL installation, credentials, native RPC or release method is invoked.
const {inventory} = await import(pathToFileURL(path.join(root,compiler)).href);
const inv = inventory();
const ordered = [...inv.migrations.map(row=>row.path),...inv.repeatables.flatMap(row=>row.paths)];
const additionalSources = ['supabase/baseline/22082026_1500_cloudtms_test_structural_baseline.sql'];
const allPaths = [...new Set([...ordered,...additionalSources])];
const batch = execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','cat-file','--batch'],{
  cwd:root,input:allPaths.map(rel=>`${expected}:${rel}`).join('\n')+'\n',maxBuffer:192*1024*1024,
});
const committed = new Map(); let offset = 0;
for (const rel of allPaths) {
  const lf=batch.indexOf(10,offset); if(lf<0)throw new Error('GIT_BATCH_HEADER_MISSING');
  const header=batch.subarray(offset,lf).toString('utf8');
  const match=/^[0-9a-f]{40} blob (\d+)$/.exec(header); if(!match)throw new Error(`GIT_BATCH_INVALID:${rel}:${header}`);
  const size=Number(match[1]);offset=lf+1;
  committed.set(rel,batch.subarray(offset,offset+size));offset+=size+1;
}
if(offset!==batch.length)throw new Error('GIT_BATCH_TRAILING_BYTES');
const wanted = new Set([
  'public.contract_week_manual_upsert_atomic','public.timesheet_authorise_generic_atomic',
  'public.timesheet_unauthorise_atomic','public.tsfin_update_segments_locked',
  'public.enqueue_ts_financials','public.tsfin_write_current_snapshot_single_bounded',
  'public.invoice_apply_edits','private.timesheet_correction_draft_round_trip_v1',
  'public.timesheet_correction_return_to_draft_v1','public.timesheet_correction_send_v1',
  'private.timesheet_correction_actor_assert_v1','private.timesheet_correction_origin_v1',
  'private.weekly_source_pay_actor_allowed_v1','private.weekly_source_agency_invoice_check_only_v1',
  'private.weekly_source_signed_check_only_v1','private.weekly_source_office_authority_v1',
  'public.weekly_source_office_authorise_scope_v1','private.weekly_source_request_cas_v1',
  'public.weekly_source_office_timesheet_presentation_v1',
  'private.timesheet_correction_invoice_scope_v1','private.timesheet_correction_draft_holds_v1',
]);
const planned = [
  'private.weekly_source_agency_draft_round_trip_v1','private.weekly_source_agency_issue_close_v1',
  'private.weekly_source_agency_week_complete_v1','public.weekly_source_agency_authorise_v1',
  'public.weekly_source_agency_invoice_whole_correction_v1','public.weekly_source_agency_timesheet_change_v1',
  'public.weekly_source_agency_week_validation_refresh_v1',
];
const definitions = new Map();
const declarationNames = new Set();
const files = new Map();
let declarationCount = 0;
for (const rel of ordered) {
  const bytes = fs.readFileSync(path.join(root,rel));
  if (!files.has(rel)) {
    if (!bytes.equals(committed.get(rel))) throw new Error(`NATIVE_SAVED_GIT_MISMATCH:${rel}`);
    files.set(rel,{path:rel,raw_sha256:sha(bytes),bytes:bytes.length});
  }
  const source = bytes.toString('utf8');
  const matcher = /\bcreate\s+(?:or\s+replace\s+)?function\s+((?:public|private)\.[a-z_0-9]+)\s*\(/ig;
  for (const match of source.matchAll(matcher)) {
    declarationCount++; declarationNames.add(match[1].toLowerCase());
    if (!wanted.has(match[1].toLowerCase())) continue;
    const remainder = source.slice(match.index);
    const body = /\bas\s+(\$[a-z_0-9]*\$)/i.exec(remainder);
    if (!body) throw new Error(`NO_DOLLAR_NATIVE_BODY:${rel}:${match[1]}`);
    const closing = remainder.indexOf(body[1],body.index+body[0].length);
    if (closing<0) throw new Error(`UNCLOSED_NATIVE_BODY:${rel}:${match[1]}`);
    const end = remainder.indexOf(';',closing+body[1].length);
    if (end<0) throw new Error(`UNCLOSED_NATIVE_STATEMENT:${rel}:${match[1]}`);
    const complete = remainder.slice(0,end+1);
    const header = remainder.slice(0,body.index).replace(/\s+/g,' ').trim();
    // Distinguish the actual Office authority overloads by their complete
    // native header. Other selected owners have one controlling signature.
    const key = match[1].toLowerCase()==='private.weekly_source_office_authority_v1'
      ? `${match[1].toLowerCase()}:${header.slice(0,header.indexOf(' returns '))}` : match[1].toLowerCase();
    definitions.set(key,{name:match[1].toLowerCase(),source_path:rel,source_line:source.slice(0,match.index).split('\n').length,
      source_raw_sha256:sha(bytes),full_header:header,definition_raw_sha256:sha(complete),definition_bytes:Buffer.byteLength(complete),complete});
  }
}
for (const name of wanted) if (![...definitions.values()].some(row=>row.name===name)) {
  if (name==='private.weekly_source_request_cas_v1') continue;
  throw new Error(`REQUIRED_NATIVE_OWNER_ABSENT:${name}`);
}
const planPath = String(process.argv[3] || '');
if (!path.isAbsolute(planPath)) throw new Error('EXACT_OWNER_PLAN_PATH_REQUIRED');
const plan = fs.readFileSync(planPath);
const out = path.join(own,'source-evidence');
fs.mkdirSync(out,{recursive:true});
const pins = [];
for (const [key,row] of definitions) {
  const filename = `${row.name.replace('.','_')}${row.name==='private.weekly_source_office_authority_v1' ? '_'+sha(key).slice(0,12) : ''}.sql.txt`;
  fs.writeFileSync(path.join(out,filename),row.complete+'\n');
  const {complete,...pin}=row;
  pins.push({...pin,evidence_path:`source-evidence/${filename}`,evidence_sha256:sha(complete+'\n')});
}
const tableNames = [
  ['private.timesheet_correction_requests','supabase/migrations/13102026_1127_healthroster_timesheet_correction_requests_v1.sql'],
  ...['public.weekly_final_source_correction_sessions','public.weekly_timesheet_submission_requests',
    'public.weekly_source_entitlement_decision_bundles','public.weekly_source_pending_entitlement_bundles']
    .map(name=>[name,'supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql']),
  ...['public.audit_events','public.ts_financials_outbox'].map(name=>[name,additionalSources[0]]),
  ['public.weekly_source_rule_set_versions','supabase/migrations/13102026_1133_weekly_source_rule_sets_schema.sql'],
];
const storePins = [];
for(const [name,rel] of tableNames){
  const bytes=fs.readFileSync(path.join(root,rel));if(!bytes.equals(committed.get(rel)))throw new Error(`STORE_PIN_CHANGED:${rel}`);
  const text=bytes.toString('utf8');const start=text.toLowerCase().indexOf('create table '+name+' (');
  const end=text.indexOf('\n);',start);if(start<0||end<0)throw new Error(`STORE_DDL_NOT_FOUND:${name}`);
  const complete=text.slice(start,end+3);const filename=name.replace('.','_')+'.table.sql.txt';
  fs.writeFileSync(path.join(out,filename),complete+'\n');
  storePins.push({name,source_path:rel,source_line:text.slice(0,start).split('\n').length,source_raw_sha256:sha(bytes),
    definition_raw_sha256:sha(complete),evidence_path:`source-evidence/${filename}`,evidence_sha256:sha(complete+'\n')});
}
const planText=plan.toString('utf8');
const planSections=[['25.2','### 25.2','## 26.'],['26.4','### 26.4','### 26.5'],['26.5','### 26.5','### 26.6'],['26.11','### 26.11','### 26.12']];
const planPins=[];
for(const [section,startToken,endToken]of planSections){
  const start=planText.indexOf(startToken);const end=planText.indexOf(endToken,start+startToken.length);
  if(start<0||end<0)throw new Error(`PLAN_SECTION_NOT_FOUND:${section}`);
  const excerpt=planText.slice(start,end);const filename=`primary_${section.replace('.','_')}.md.txt`;
  fs.writeFileSync(path.join(out,filename),excerpt);
  planPins.push({section,start_line:planText.slice(0,start).split('\n').length,evidence_path:`source-evidence/${filename}`,evidence_sha256:sha(excerpt)});
}
const facts = {
  schema:'AGENCY_CURRENT_SOURCE_DESIGN_INPUTS_V1',source_only:true,actual_current_git_commit:expected,
  owner_plan:{path:planPath,raw_sha256:sha(plan),bytes:plan.length},
  pure_inventory_owner:{path:compiler,raw_sha256:sha(compilerBytes),only_methods_called:['inventory']},
  inventory:{migrations:inv.migrations.length,repeatables:inv.repeatables.length,ordered_occurrences:ordered.length,
    distinct_inputs:files.size,function_declaration_occurrences:declarationCount,distinct_callable_names:declarationNames.size},
  planned_names:planned.map(name=>({name,declaration_found:declarationNames.has(name),semantic_verdict:'SEE_SOURCE_SEMANTIC_MAP'})),
  native_owners:pins,
  closest_and_native_store_definitions:storePins,
  controlling_plan_sections:planPins,
  runtime_inputs:['broker/src/index.js','broker/src/weekly-source/authorise-routing.mjs','broker/src/payment-actor-permission.js'].map(rel=>{
    const bytes=fs.readFileSync(path.join(root,rel));if(!bytes.equals(git('show',`${expected}:${rel}`)))throw new Error(`RUNTIME_PIN_CHANGED:${rel}`);
    return {path:rel,raw_sha256:sha(bytes),bytes:bytes.length};
  }),
  native_calls:false,database_calls:false,compiled_contract_sha256:null,qualification_receipt:null,
};
fs.writeFileSync(path.join(own,'SOURCE_INPUTS.json'),JSON.stringify(facts,null,2)+'\n');
console.log(JSON.stringify({source_pin:expected,inventory:facts.inventory,native_owner_count:pins.length,planned_absent:facts.planned_names.filter(row=>!row.declaration_found).length},null,2));
