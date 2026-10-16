import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {scanManagedSql} from '../../../../scripts/cloudtms-managed-release-sql.mjs';
import {sqlDateKey} from '../../../../scripts/cloudtms-db-release-lib.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..');
const pin='a934ee1d460b444939234e812babccb90dc3ddff';
const git=(...args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],{cwd:repo,maxBuffer:512*1024*1024});
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const paths=git('ls-tree','-r','--name-only',pin,'--','supabase/migrations','supabase/repeatable','supabase/baseline').toString().trim().split('\n').filter(Boolean);
const batch=execFileSync('git',['cat-file','--batch'],{cwd:repo,input:paths.map(p=>`${pin}:${p}`).join('\n')+'\n',maxBuffer:512*1024*1024});
const bytes=new Map();let off=0;for(const file of paths){const end=batch.indexOf(10,off),m=batch.subarray(off,end).toString().match(/^[0-9a-f]{40} blob (\d+)$/);assert.ok(m,file);off=end+1;const size=Number(m[1]);bytes.set(file,batch.subarray(off,off+size));off+=size+1;}assert.equal(off,batch.length);
const sort=(a,b)=>sqlDateKey(a).localeCompare(sqlDateKey(b))||a.localeCompare(b),ordered=paths.filter(p=>p.endsWith('.sql')&&(p.startsWith('supabase/baseline/')||p.startsWith('supabase/migrations/'))).sort(sort);
function visit(file,active=new Set()){assert.ok(!active.has(file),file);active.add(file);ordered.push(file);for(const line of bytes.get(file).toString().split(/\r?\n/)){const m=line.match(/^\s*\\ir\s+(?:'([^']+)'|"([^"]+)"|([^\s;]+))\s*;?\s*$/);if(m)visit(path.posix.normalize(path.posix.join(path.posix.dirname(file),m[1]??m[2]??m[3])),active);}active.delete(file);}
for(const p of paths.filter(p=>p.startsWith('supabase/repeatable/')&&p.endsWith('.sql')).sort(sort))visit(p);
const names=new Set([
 'private._invoice_operation_start_core_v8',
 'private.weekly_source_invoice_family_timesheet_ids_v1','private.weekly_source_invoice_movement_only_integrity_v1','private.weekly_source_invoice_line_owner_guard_v1',
 'public.weekly_source_invoice_batch_candidates_v1','public.weekly_source_invoice_batch_admit_atomic_v1','public.weekly_source_invoice_admit_atomic_v1','private.weekly_source_invoice_allocation_assert_v1','private.weekly_source_invoice_issue_validate_v1','private.weekly_source_invoice_movement_money_v1','public.weekly_source_invoice_report_rows_v1','public.weekly_source_invoice_evidence_v1','private.weekly_source_invoice_receipt_support_v1','private.weekly_source_invoice_admitted_member_money_v1','private.weekly_source_invoice_duty_v1','private._invoice_presentation_snapshot_batch','public.invoice_render_manifest','public.invoice_line_archived_timesheet_guard_v1','public.tsfin_report_timesheets_v2','public.tsfin_search_timesheets_v2','public.timesheet_summary_lightweight_rows_v1','private.weekly_source_finalisation_lineage_assert_v1','private.weekly_source_finalisation_economic_assert_v1','private.weekly_source_agency_source_dependencies_v1','private.weekly_source_sha256_jsonb_v1','public._pay_timesheet_rotation_scope',
 'private._candidate_expense_current_timesheet_id_v1','private._candidate_expense_owned_timesheet_id_v1','private._candidate_expense_owner_context_lock_v1','private._candidate_expense_components_sync_financial_v1','private._candidate_expense_components_sync_v1',
 'private.invoice_credit_scope_context_v1','private.invoice_credit_scoped_advance_v1','private._invoice_credit_resume_v1','private._invoice_generation_advance_batch','private._invoice_generation_advance_core_v8','private._invoice_generation_resolve_command_groups','private._invoice_issue_advance_core_v8','public.invoice_operation_advance_batch','public.invoice_issue_batch','public.invoice_issue_one','public.invoice_unissue','public.invoice_apply_edits','private._invoice_issue_validate_batch','private._invoice_correction_validate_batch','public.invoice_batch_generate_candidates_v2','private._invoice_batch_generate_candidate_rows_v2','private._invoice_batch_generate_group_rows_v2'
]);
const owners=new Map();for(const file of ordered){const parsed=scanManagedSql(bytes.get(file).toString());for(const st of parsed.statements){const body=parsed.source.slice(st.start,st.end),m=body.match(/^create\s+(?:or\s+replace\s+)?function\s+(["a-z0-9_.]+)\s*\(([\s\S]*?)\)\s*returns\b/i);if(!m)continue;let name=m[1].replaceAll('"','').toLowerCase();if(!name.includes('.'))name='public.'+name;if(names.has(name))owners.set(name,{name,path:file,signature:m[2].replace(/\s+/g,' ').trim(),definition:body});}}
fs.mkdirSync(path.join(here,'reference'),{recursive:true});
const manifest=[];for(const row of owners.values()){const file='reference/'+row.name.replace('.','_')+'.sqlinc';fs.writeFileSync(path.join(here,file),row.definition+'\n');const {definition,...record}=row;manifest.push({...record,file,sha256:sha(definition)});}
fs.writeFileSync(path.join(here,'CURRENT_NATIVE_INPUTS.json'),JSON.stringify({pin,native:false,owners:manifest,missing:[...names].filter(n=>!owners.has(n))},null,2)+'\n');
const schema=[];for(const file of paths.filter(p=>p.endsWith('.sql')&&(p.startsWith('supabase/baseline/')||p.startsWith('supabase/migrations/')))){const parsed=scanManagedSql(bytes.get(file).toString());for(const st of parsed.statements){const body=parsed.source.slice(st.start,st.end);if(/^(?:create\s+(?:unlogged\s+)?table|alter\s+table)\b/i.test(body)&&/\b(?:invoices|invoice_lines|invoice_operations|invoice_operation_chunks|invoice_document_versions|invoice_hr_source_rows|candidate_expense_components|candidate_expense_claims|timesheets_financials)\b/i.test(body.slice(0,250)))schema.push('-- '+file+'\n'+body);}}
fs.writeFileSync(path.join(here,'reference/native-schema.sqlinc'),schema.join('\n\n')+'\n');
fs.writeFileSync(path.join(here,'reference/scoped_snapshot_predecessor.sqlinc'),git('show',pin+':supabase/repeatable/13102026_1143_healthroster_scoped_credit/13102026_1143_snapshot.sqlinc'));
console.log(JSON.stringify({pin,selected:manifest.length,missing:[...names].filter(n=>!owners.has(n))}));
