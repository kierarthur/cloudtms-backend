import {readFileSync,writeFileSync,readdirSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {scanManagedSql,exactAtomicBody} from '../../../../scripts/cloudtms-managed-release-sql.mjs';
const base=path.dirname(fileURLToPath(import.meta.url));const root=path.resolve(base,'../../../..');
const commit='90ffcf3f40644280fcdb5b791a62a86613896d6e';const sha=s=>createHash('sha256').update(s).digest('hex');
const files=['supabase/baseline/22082026_1500_cloudtms_test_structural_baseline.sql',
 'supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql','supabase/migrations/15092026_2310_weekly_source_delivery_targets.sql',
 'supabase/migrations/15102026_0802_weekly_contact_pause_permits.sql',
 'supabase/repeatable/06102026_1214_self_bill_candidate_expense_separation_v1.sql',
 'supabase/repeatable/15092026_1534_weekly_source_settings_expenses_rates_v1.sql',
 'supabase/repeatable/includes/weekly_contact_source_memberships/010_native_logical_members.sqlinc',
 'supabase/repeatable/includes/weekly_contact_source_memberships/020_native_budget.sqlinc',
 'supabase/repeatable/includes/weekly_contact_timing/040_native_timing.sqlinc',
 'supabase/repeatable/15102026_0803_candidate_push_dispatch_read_v1.sql',
 'broker/src/weekly-source/push-timing-office.mjs'];
const inputs=files.map(file=>({path:file,sha256_normalized_lf:sha(execFileSync('git',['show',`${commit}:${file}`],{cwd:root,encoding:'utf8',maxBuffer:8e6}).replace(/\r\n/g,'\n'))}));
const acl=[];
for(const file of readdirSync(path.join(base,'forward')).filter(x=>x.endsWith('.sqlinc')&&!x.endsWith('_acl.sqlinc'))){
 const sql=readFileSync(path.join(base,'forward',file),'utf8');
 for(const m of sql.matchAll(/create or replace function ((public|private)\.[a-z0-9_]+)\(([\s\S]*?)\)\s*returns/gi)){
  const args=m[3].trim()?m[3].split(',').map(s=>s.trim().split(/\s+/)[1]).join(','):'';
  const signature=m[1]+'('+args+')';
  acl.push(`alter function ${signature} owner to postgres;\nrevoke all on function ${signature} from public,anon,authenticated,service_role;`
   +(m[2]==='public'?`\ngrant execute on function ${signature} to service_role;`:''));
 }
}
writeFileSync(path.join(base,'forward/99_acl.sqlinc'),acl.join('\n')+'\n');
const outputs=[...readdirSync(path.join(base,'one-time')).filter(x=>x.endsWith('.sqlinc')).sort().map(x=>'one-time/'+x),...readdirSync(path.join(base,'forward')).filter(x=>x.endsWith('.sqlinc')).sort().map(x=>'forward/'+x),
 'runtime/contact-manual-enquiry.mjs','runtime/contact-manual-enquiry.d.ts','MANUAL_RUNTIME_CONTRACT_RECEIPT.json',
 'NATIVE_CONTINUATION_WIP.md','NATIVE_OWNER_INPUTS.json','REGISTER_DELTA_WIP.json',
 'build-current-owners.mjs','build-read-checkpoint.mjs','tests/manual-request.test.mjs'].map(file=>({path:file,sha256:sha(readFileSync(path.join(base,file)))}));
const atomic='begin;\n'+outputs.filter(x=>x.path.endsWith('.sqlinc')).map(x=>readFileSync(path.join(base,x.path),'utf8')).join('\n')+'\ncommit;\n';
const statements=scanManagedSql(exactAtomicBody(atomic,{allowAtomicBody:false})).statements.length;
writeFileSync(path.join(base,'READ_SOURCE_CHECKPOINT.json'),JSON.stringify({version:'CONTACT_COMMISSIONED_PRODUCERS_READ_CHECKPOINT_V1',base_commit:commit,inputs,outputs,
 sql_statement_count:statements,private_owner_declarations:acl.length,
 status:'NOT_FOR_MOUNT_NOT_REVIEWED_NATIVE_REQUIRED_NOT_RUN',
 readonly_row_owner:'private.weekly_contact_manual_enquiry_read_v1(uuid,text,uuid,uuid)',
 native:'NOT_RUN; managed scanner treats PL/pgSQL bodies as opaque',
 unfinished:['Configured MANUAL-step actual SEND_MANAGER_NOW/bulk projection/CAS successor',
  'Bounded existing recheck-task consumer/progress/cursor/future-wake + genuine legacy outbox phase',
  'Unchanged capture replay/coalescing, due rebase integrity, aliases and actual notification fallback',
  'Bounded actual scheduler/runtime followup/recheck invocation with existing fanout/failure semantics',
  'Hostile native fixtures and actual root-owned PostgreSQL/ACL/concurrency/provider-free acceptance'],
 source_only_tests:'manual-request.test.mjs uses actual current shared JSON parser; no database/provider/native proof',
 database_actions:'NONE'},null,2)+'\n');
console.log(JSON.stringify({private_owner_declarations:acl.length,statements,status:'IN_PROGRESS_NATIVE_REQUIRED_NOT_RUN'}));
