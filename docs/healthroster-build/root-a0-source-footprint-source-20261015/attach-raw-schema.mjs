import fs from 'node:fs';import cp from 'node:child_process';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';
import {scanManagedSql} from '../scripts/cloudtms-managed-release-sql.mjs';
const base='docs/healthroster-build/proposals/a0-source-raw-selector-guard-construction-20261015';
const source='fa66183f01913e86651e82816f5e88f97e07123c',commit='63965738af6f148a75cd1bea5052600e4b007da5';
const input='docs/healthroster-build/proposals/agency-source-dependency-current-reader-forward-20261014/construction/SOURCE_RAW_ID_DDL_MAPPING.json';
const sha=b=>createHash('sha256').update(b).digest('hex');
const raw=cp.execFileSync('git',['cat-file','blob',commit+':'+input],{maxBuffer:4e6}),mapping=JSON.parse(raw);
const rulesPath='supabase/migrations/13102026_1133_weekly_source_rule_sets_schema.sql';
const rules=cp.execFileSync('git',['cat-file','blob',source+':'+rulesPath],{maxBuffer:4e6});
const parsedRules=scanManagedSql(rules.toString());
const rulesStatement=parsedRules.statements.map(s=>parsedRules.source.slice(s.start,s.end)).find(s=>/^create table public\.weekly_source_rule_sets\s*\(/i.test(s));assert.ok(rulesStatement);
const additionalTable={relation:'public.weekly_source_rule_sets',ddl:[{source_path:rulesPath,source_line:7,statement_raw_sha256:sha(rulesStatement),statement:rulesStatement}]};
for(const p of mapping.source_pins){const b=cp.execFileSync('git',['cat-file','blob',source+':'+p.path],{maxBuffer:4e6});assert.equal(sha(b),p.raw_sha256,p.path);}
const relations=JSON.parse(fs.readFileSync(base+'/reference/SOURCE_OBJECT_RELATIONS.json'));
const required=new Map(relations.map(relation=>[relation,[[relation==='public.timesheets'?'timesheet_id':'id','uuid']]]));
required.set('public.weekly_source_global_settings',[['singleton','boolean']]);
const more={
 weekly_source_upload_rows:[['upload_id','uuid']],weekly_source_expense_cell_evidence:[['upload_id','uuid']],
 weekly_source_row_economic_snapshots:[['row_resolution_id','uuid']],weekly_source_row_expense_policy_snapshots:[['row_resolution_id','uuid']],
 weekly_source_charge_checks:[['row_resolution_id','uuid']],weekly_source_charge_acceptances:[['row_resolution_id','uuid']],
 weekly_source_invoice_hold_releases:[['upload_row_id','uuid']],weekly_source_contract_policies:[['contract_id','uuid']],
 client_settings:[['client_id','uuid']],weekly_source_group_rule_assignments:[['source_group_id','uuid']],
 weekly_source_self_bill_report_line_bindings:[['state','text'],['self_bill_report_id','uuid']],
 weekly_source_row_resolutions:[['upload_row_id','uuid'],['generation','integer'],['client_id','uuid']],
 weekly_source_final_snapshot_lines:[['final_revision_id','uuid'],['client_id','uuid']],weekly_source_ordinary_pay_projection_receipts:[['final_revision_id','uuid'],['client_id','uuid']],
 weekly_expense_authority_generations:[['final_revision_id','uuid'],['work_event_id','uuid']],weekly_source_state_transitions:[['final_revision_id','uuid'],['work_event_id','uuid']],
 weekly_source_group_clients:[['source_group_id','uuid'],['client_id','uuid']],weekly_source_client_policies:[['source_group_id','uuid'],['client_id','uuid']],
 weekly_source_self_bill_reports:[['source_group_id','uuid'],['client_id','uuid']],weekly_source_report_scopes:[['source_group_id','uuid'],['client_id','uuid'],['self_bill_report_id','uuid']],
 weekly_discrepancy_incidents:[['source_cycle_id','uuid'],['client_id','uuid']],contracts:[['client_id','uuid'],['start_date','date'],['end_date','date']]
};
for(const [table,columns] of Object.entries(more))required.get('public.'+table).push(...columns);
const evidence=[];for(const [relation,columns] of required){const table=mapping.tables.find(x=>x.relation===relation)||(relation===additionalTable.relation?additionalTable:null);assert.ok(table,relation);for(const [column,type] of columns){
 const pattern=new RegExp('(?:^|[\\s,])"?'+column+'"?\\s+(?:pg_catalog\\.)?'+type+'\\b','i');
 const ddl=table.ddl.filter(x=>pattern.test(x.statement));assert.ok(ddl.length,relation+'.'+column+' '+type);
 evidence.push({relation,column,type,declarations:ddl.map(x=>({source_path:x.source_path,source_line:x.source_line,statement_raw_sha256:x.statement_raw_sha256}))});
}}
fs.writeFileSync(base+'/reference/SOURCE_RAW_ID_DDL_MAPPING.json',raw,{flag:'wx'});
fs.writeFileSync(base+'/reference/13102026_1133_weekly_source_rule_sets_schema.sql.txt',rules,{flag:'wx'});
fs.writeFileSync(base+'/RAW_COLUMN_SOURCE_EVIDENCE.json',JSON.stringify({source_only:true,native:false,source_commit:source,mapping_commit:commit,mapping_input:input,mapping_sha256:sha(raw),verified_current_source_pins:mapping.source_pins.length,additional_current_rules_schema:{path:rulesPath,sha256:sha(rules)},checkpoint_note:'Initial older mapping lacks new rule-set header; exact current1133 CREATE supplements its declaration. No relation skipped.',required_relations:required.size,raw_column_evidence:evidence,compiled_catalogue:false,limitations:['literal raw CREATE/ALTER declarations; no compiled physical catalogue or late dynamic-DDL proof','nested revision/lineage and transitive dependencies remain unimplemented']},null,2)+'\n',{flag:'wx'});
const pins=JSON.parse(fs.readFileSync(base+'/INPUT_PINS.json'));pins.pins.push({file:'reference/SOURCE_RAW_ID_DDL_MAPPING.json',source_commit:commit,source_path:input,bytes:raw.length,sha256:sha(raw)},{file:'reference/13102026_1133_weekly_source_rule_sets_schema.sql.txt',source_commit:source,source_path:rulesPath,bytes:rules.length,sha256:sha(rules)});fs.writeFileSync(base+'/INPUT_PINS.json',JSON.stringify(pins,null,2)+'\n');
const artifacts=fs.readdirSync(base,{recursive:true}).filter(f=>f!=='SOURCE_MANIFEST.json'&&fs.statSync(base+'/'+f).isFile()).sort().map(file=>{const b=fs.readFileSync(base+'/'+file);return{file:file.replaceAll('\\','/'),bytes:b.length,sha256:sha(b)};});fs.writeFileSync(base+'/SOURCE_MANIFEST.json',JSON.stringify({source_only:true,native:false,mount_ready:false,artifacts},null,2)+'\n');
console.log(JSON.stringify({source_only:true,raw_relations:required.size,raw_columns:evidence.length,verified_current_pins:mapping.source_pins.length,artifacts:artifacts.length}));
