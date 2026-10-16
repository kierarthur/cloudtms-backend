// Read-only native source inventory. Writes only this proposal's fixture record.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const fixture=path.dirname(fileURLToPath(import.meta.url));
const root=JSON.parse(fs.readFileSync(path.join(fixture,'../SOURCE_INPUTS.json'),'utf8')).root;
const git=(...args)=>execFileSync('git',args,{cwd:root,encoding:'utf8'}).trim();
const headBefore=git('rev-parse','HEAD');
const sha=bytes=>crypto.createHash('sha256').update(bytes).digest('hex');
const seeds=[
 'supabase/repeatable/21072026_1820_00_import_review_internal_core.sql',
 'supabase/repeatable/21072026_1820_01_import_review_lifecycle_rpcs.sql',
 'supabase/repeatable/21072026_1820_04_timesheet_query_email_rpcs.sql',
 'supabase/repeatable/21072026_1820_06_hr_weekly_apply_transactional.sql',
 'supabase/repeatable/13102026_1205_healthroster_issued_signed_query_v1.sql',
 'supabase/repeatable/13102026_1134_weekly_source_rule_sets_v1.sql',
 'supabase/repeatable/13102026_1127_healthroster_correction_requests/13102026_1127_context.sqlinc',
 'supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql',
 'supabase/repeatable/23072026_2207_email_outbox_claim_ready_batch.sql',
 'supabase/repeatable/15102026_0802_weekly_contact_pause_v1.sql',
 'supabase/repeatable/16102026_0214_weekly_contact_source_admission_v1.sql',
 'supabase/repeatable/16102026_0226_weekly_contact_query_outbox_admission_v1.sql',
 'supabase/repeatable/16102026_0339_weekly_contact_query_native_lifecycle_v1.sql',
 'supabase/migrations/16102026_0339_weekly_contact_query_lineage_and_claim_fence.sql',
 'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1042_11_private_invoice_operation_start_core_v8.sql',
 'supabase/repeatable/13102026_1143_healthroster_scoped_credit/13102026_1143_start.sqlinc',
 'supabase/repeatable/13102026_1143_healthroster_scoped_credit/13102026_1143_advance.sqlinc',
 'supabase/repeatable/13102026_1143_healthroster_scoped_credit/13102026_1143_planner.sqlinc',
 'supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/12_retained_final_issue_transport.sqlinc',
 'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_trg_timesheet_document_invalidate.sql',
 'broker/src/invoice-async-http.js','broker/src/invoice-queue-runtime.js','broker/src/index.js'
];
const files=new Map();
function capture(relative){
 const absolute=path.resolve(root,relative);
 const normalized=path.relative(root,absolute).replaceAll('\\','/');
 if(normalized.startsWith('../')||path.isAbsolute(normalized))throw new Error('Native dependency leaves exact root');
 if(files.has(normalized))return;
 const bytes=fs.readFileSync(absolute),source=bytes.toString('utf8');
 const includes=[...source.matchAll(/^\\ir\s+([^\r\n]+)$/gm)].map(x=>path.resolve(path.dirname(absolute),x[1].trim()));
 files.set(normalized,{file:normalized,sha256:sha(bytes),git_saved_status:git('status','--short','--',normalized)||'CLEAN',
  include_files:includes.map(x=>path.relative(root,x).replaceAll('\\','/'))});
 for(const include of includes)capture(path.relative(root,include));
}
for(const seed of seeds)capture(seed);
if(git('rev-parse','HEAD')!==headBefore)throw new Error('Native root HEAD changed during source capture; retry exact read-only capture');
const result={version:'QUERY_NATIVE_PREFIX_SOURCE_INPUTS_V1',root,root_head:headBefore,database_execution:false,
 fixture_installation:'PG_TEMP_ONLY_IN_ROOT_SERVER_OWNED_TEST_ROLLBACK',native:'REQUIRED_NOT_RUN',
 owners:{ingress:['broker/src/index.js:findImportStageReplay','broker/src/index.js:parseHealthRosterWorkbookIntoHrRows','broker/src/index.js:handleHrAutoprocessImport'],
  review:['public.import_review_create_v1','public.import_review_save_v1','public.import_review_get_v1','public.hr_weekly_apply_transactional','public._import_review_apply_complete_core_v1'],
  evidence:['private.weekly_source_issued_signed_query_evidence_v1','private.timesheet_correction_signed_witness_v1','public._import_review_query_evidence_core_v1'],
  native_query:['public.timesheet_query_email_enqueue_v1','public.email_outbox_claim_ready_batch','public.email_outbox_claim_ready_batch_contact_v1','public.weekly_contact_submission_start_v1','public.timesheet_query_email_contact_result_v1'],
  document:['private._invoice_operation_start_core_v8','private._invoice_document_advance_batch','public.trg_timesheet_document_invalidate','broker/src/invoice-async-http.js','broker/src/invoice-queue-runtime.js']},
 inputs:[...files.values()].sort((a,b)=>a.file.localeCompare(b.file)),
 limits:['Source bytes and signatures only; no PostgreSQL binding or execution proof.','Input discovery returns candidates; actual review catalogue decides eligibility.',
 'Existing signed/issued/current READY metadata is consumed, never fabricated; R2 object/route and external acceptance remain unverified.',
 'Original generic claim is used only before query migration/bridge exists; real legacy snapshot is checked after atomic installation.',
 'Fresh native leases cannot become reclaimable inside one transaction because original claim uses transaction_timestamp(); older expiry cases require genuine older inputs.']};
fs.writeFileSync(path.join(fixture,'QUERY_NATIVE_PREFIX_INPUTS.json'),JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify({root_head:result.root_head,files:result.inputs.length,dirty:result.inputs.filter(x=>x.git_saved_status!=='CLEAN').length,native:result.native}));
