// Source-only extraction. This module has no database, release or runtime action.
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { scanManagedSql } from '../../../../scripts/cloudtms-managed-release-sql.mjs';

const folder=path.dirname(fileURLToPath(import.meta.url));
const root=path.resolve(folder,'../../../..');
const common='e0a02bb5f86b1fb2134e540bbf0ccc52f6feec92';
const actual='37ddadc0a50721834c9e8a6c06becfc983f0dc32';
const broad='2151af656561ad4d68e61e242769ed80a631fcc9';
const hash=bytes=>createHash('sha256').update(bytes).digest('hex');
const git=args=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{cwd:root,maxBuffer:300_000_000});
const object=(commit,file)=>git(['show',commit+':'+file]);
const targets=[
 ['public.weekly_exceptional_pay_complete_local_v1','supabase/repeatable/14102026_0044_weekly_source_protected_pay_authority_v2.sql','supabase/repeatable/13102026_2205_weekly_source_automatic_native_delta/09_complete_local_successor.sqlinc','6330a93faec20e7a1e6ffa421dbd2535daebbd58f77599eaa5b41a8c3614c04b','06fba0c98a138264de1236a8077e1b02ff455470f7d138502b535504a52741ea'],
 ['public.weekly_source_manual_review_resolve_v1','supabase/repeatable/11102026_2337_banking_pay_next_source_query_workbench_refresh_v1.sql','supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/10b_complete_remaining_manifest_consumers.sqlinc','a522deee42487d3a582dda9d74f3c270a186d69d9659f0c31978589005c9a282','81c8f3d79a8f26876345f81c487816d9b9f4eb6827c4727fa8170c706dbb2387'],
 ['public.weekly_exceptional_pay_stage_c1_request_v1','supabase/repeatable/10102026_0853_weekly_source_pay_query_entry_admission_v1.sql','supabase/repeatable/13102026_1438_weekly_source_protection_queries/20_stage_request.sqlinc','2c311b23730e311166f5df8ebd86cbc051579352f43693720c142f8c195b4ff2','3b3dde1568fa011c2a63643ce998de993f2abb7ee3499f34c6bdcf75c688a25a'],
 ['private.weekly_source_candidate_submission_compare_sync_v1','supabase/repeatable/11102026_2337_banking_pay_next_source_query_workbench_refresh_v1.sql','supabase/repeatable/13102026_1755_weekly_source_publication_comparison_family_union_v1.sql','8c5dba0fd51cb20504c3a06f6bca0f4e15c9bb7e36753cf48b9c7d251969539a','570ef77bc826fbfca46c4ecda1f88ed2fd4768d8eea5d4edd674214e585e5c4b'],
 ['private.weekly_source_manual_reviews_finalised_v1','supabase/repeatable/11102026_2337_banking_pay_next_source_query_workbench_refresh_v1.sql','supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/10b_complete_remaining_manifest_consumers.sqlinc','aca683c58b274102191ef9044bd1a9471d0c23ba86d3a5bfc017bff04539de69','c3cda6465baab128e280647714ae62aeb1a6d92c700b043a79918cb84cc8c78c']
];
const names=new Set(targets.map(row=>row[0]));
function definitions(bytes){const parsed=scanManagedSql(bytes.toString('utf8'));return parsed.statements.flatMap(unit=>{
 const definition=parsed.source.slice(unit.start,unit.end);
 const head=definition.match(/^\s*create\s+(?:or\s+replace\s+)?function\s+((?:private|public)\.\w+)\s*\(/i);
 return head&&names.has(head[1].toLowerCase())?[{name:head[1].toLowerCase(),definition,sha256:hash(definition)}]:[];
});}
function one(commit,file,name,expected){const rows=definitions(object(commit,file)).filter(row=>row.name===name);assert.equal(rows.length,1,name);assert.equal(rows[0].sha256,expected,name);return {...rows[0],commit,file};}
const manifest=JSON.parse(object(common,'supabase/release/current-release.json'));
const files=git(['ls-tree','-r','--name-only',common]).toString('utf8').trim().split('\n');
const dateSource=object(common,'scripts/cloudtms-db-release-lib.mjs').toString('utf8');
const dateDeclaration=dateSource.slice(dateSource.indexOf('export function sqlDateKey'),dateSource.indexOf('export function sqlFiles'));
// The exact repository date-order declaration is local pure code.
const sqlDateKey=Function('path',dateDeclaration.replace('export function','function')+';return sqlDateKey;')(path);
const chronological=list=>list.sort((a,b)=>sqlDateKey(a).localeCompare(sqlDateKey(b))||a.localeCompare(b));
const roots=[...manifest.baselineFiles,...chronological(files.filter(file=>/^supabase\/migrations\/.*\.sql$/.test(file))),...chronological(files.filter(file=>/^supabase\/repeatable\/.*\.sql$/.test(file)))];
const inputs=new Map(),final=new Map();
function visit(file,stack=[]){assert.ok(!stack.includes(file),'INCLUDE_CYCLE');const bytes=object(common,file);inputs.set(file,{file,sha256:hash(bytes)});const parsed=scanManagedSql(bytes.toString('utf8'));
 for(const unit of [...parsed.statements.map(row=>({...row,kind:'statement'})),...parsed.meta.map(row=>({...row,kind:'meta'}))].sort((a,b)=>a.start-b.start)){
  const body=parsed.source.slice(unit.start,unit.end);
  if(unit.kind==='meta'){const include=body.match(/^\s*\\ir\s+(?:'([^']+)'|"([^"]+)"|([^\s;]+))/i);if(include)visit(path.posix.normalize(path.posix.join(path.posix.dirname(file),include[1]??include[2]??include[3])),[...stack,file]);continue;}
  for(const row of definitions(Buffer.from(body)))final.set(row.name,{...row,commit:common,file});
 }
}
for(const file of roots)visit(file);
const rows=[];
for(const [name,actualFile,broadFile,actualHash,broadHash] of targets){const current=one(actual,actualFile,name,actualHash),proposed=one(broad,broadFile,name,broadHash),base=final.get(name);assert.ok(base,name+' COMMON_OWNER_REQUIRED');
 const slug=name.replace(/^(?:private|public)\./,'');
 for(const [label,row] of [['actual',current],['broad',proposed],['common',base]])fs.writeFileSync(path.join(folder,slug+'.'+label+'.sqlinc'),row.definition+'\n');
 rows.push({name,slug,actual:current,broad:proposed,common:base});
}
fs.writeFileSync(path.join(folder,'INPUTS.json'),JSON.stringify({sourceOnly:true,status:'EXTRACTED_INPUTS_NOT_COMPOSED',actualCommit:actual,broadCommit:broad,commonCommit:common,dateOrderSha256:hash(dateDeclaration),commonRoots:roots,commonInputs:[...inputs.values()],owners:rows},null,2)+'\n');
console.log(JSON.stringify({sourceOnly:true,commonRoots:roots.length,commonInputPaths:inputs.size,owners:rows.map(({name,actual,broad,common})=>({name,actual:actual.sha256,broad:broad.sha256,common:common.sha256}))}));
