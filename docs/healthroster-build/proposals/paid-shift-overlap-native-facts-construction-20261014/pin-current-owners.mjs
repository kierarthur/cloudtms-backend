import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {functionEvents,applyFunctionEvents,emptyFunctionIndex} from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
import {scanManagedSql} from '../strict-qualification-install1-forward/PINNED_MANAGED_SQL_SCANNER.mjs';
import {sqlDateKey} from '../strict-qualification-install1-forward/PINNED_SQL_DATE_ORDER.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),commit='fa66183f01913e86651e82816f5e88f97e07123c',sha=b=>createHash('sha256').update(b).digest('hex');
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));
const git=(args,input)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],{cwd:root,input,maxBuffer:40e6}),object=f=>git(['show',commit+':'+f]);
const scanner=object('scripts/cloudtms-managed-release-sql.mjs');assert.deepEqual(scanner,fs.readFileSync(path.join(dir,'../strict-qualification-install1-forward/PINNED_MANAGED_SQL_SCANNER.mjs')));
const lib=object('scripts/cloudtms-db-release-lib.mjs').toString(),date=lib.slice(lib.indexOf('export function sqlDateKey'),lib.indexOf('export function sqlFiles'));
assert.equal("import path from 'node:path';\n"+date,fs.readFileSync(path.join(dir,'../strict-qualification-install1-forward/PINNED_SQL_DATE_ORDER.mjs'),'utf8'));
const files=git(['ls-tree','-r','--name-only',commit,'--','supabase']).toString().trim().split('\n').filter(f=>/\.(sql|sqlinc|inc)$/.test(f)||f==='supabase/release/current-release.json'),cache=new Map();
for(let i=0;i<files.length;i+=64){const batch=files.slice(i,i+64),raw=git(['cat-file','--batch'],batch.map(f=>commit+':'+f+'\n').join(''));let cursor=0;
  for(const file of batch){const end=raw.indexOf(10,cursor),header=raw.subarray(cursor,end).toString().split(' ');assert.equal(header[1],'blob',file);const bytes=Number(header[2]);cursor=end+1;cache.set(file,raw.subarray(cursor,cursor+bytes));cursor+=bytes+1;}assert.equal(cursor,raw.length);
}
const release=JSON.parse(cache.get('supabase/release/current-release.json')),chronological=rows=>rows.sort((a,b)=>sqlDateKey(a).localeCompare(sqlDateKey(b))||a.localeCompare(b)),roots=[...release.baselineFiles,...chronological(files.filter(f=>/^supabase\/migrations\/.*\.sql$/.test(f))),...chronological(files.filter(f=>/^supabase\/repeatable\/.*\.sql$/.test(f)))],index=emptyFunctionIndex(),occurrences=[],inputs=new Map();let order=0;
function visit(file,stack=[]){assert.ok(cache.has(file),file);assert.ok(!stack.includes(file));const bytes=cache.get(file),text=bytes.toString().replaceAll('\r\n','\n'),parsed=scanManagedSql(text);inputs.set(file,{file,bytes:bytes.length,sha256:sha(bytes)});occurrences.push({file,stack,sha256:sha(bytes)});
  for(const unit of [...parsed.statements.map(r=>({...r,kind:'statement'})),...parsed.meta.map(r=>({...r,kind:'meta'}))].sort((a,b)=>a.start-b.start)){
    const body=text.slice(unit.start,unit.end);if(unit.kind==='meta'){const m=body.match(/^\s*\\ir\s+(?:'([^']+)'|"([^"]+)"|([^\s;]+))/);if(m)visit(path.posix.normalize(path.posix.join(path.posix.dirname(file),m[1]??m[2]??m[3])),[...stack,file]);continue;}
    const parsedFunctions=functionEvents(body);index.unresolved.push(...parsedFunctions.unresolved.map(r=>({...r,file,order:order++})));
    applyFunctionEvents(index,parsedFunctions.events,e=>({...e,file,include_stack:stack,order:order++,source_file_sha256:sha(bytes),definition_sha256:sha(e.definition??body)}));
  }
}
for(const file of roots)visit(file);
const helper=fs.readFileSync(path.join(dir,'forward/01_work_event_payment_facts.sqlinc'),'utf8'),calls=[...new Set([...helper.matchAll(/\b((?:private|public)\.\w+)\s*\(/g)].map(m=>m[1]))].filter(n=>!n.includes('work_event_payment_facts')&&!n.includes('sha256'));
const selectedNames=new Set([...calls,'private.weekly_source_finalise_engine_v1','private.bpay_next_source_current_capture_transition_v1','private.weekly_source_ordinary_projection_active_movements_v1','public.bpay_next_source_paid_evidence_page_v1','private.weekly_source_correct_final_prepared_context_v1','public.weekly_source_finalisation_pay_open_atomic_v1']);
const selected=[...index.live.values()].filter(r=>selectedNames.has(r.name)).map(({definition,body,...r})=>({...r,body_sha256:sha(body)}));
assert.ok(selected.some(r=>r.id==='private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean,text)'&&r.file==='supabase/repeatable/14102026_0821_healthroster_fin4_rebill_record_fields/01_complete_fin4.sqlinc'));
fs.writeFileSync(path.join(dir,'CURRENT_EFFECTIVE_OWNER_PINS.json'),JSON.stringify({source_only:true,commit,canonical_order:'actual baselineFiles then all recursive .sql roots by actual sqlDateKey, every literal include occurrence, semantic CREATE/DROP/RENAME handling',all_sql_roots:roots.length,visited_input_paths:inputs.size,source_occurrences:occurrences.length,final_literal_semantic_functions:index.live.size,unresolved_dynamic_ddl_count:index.unresolved.length,unresolved_is_native_hold:true,scanner_sha256:sha(scanner),date_order_sha256:sha(date),selected_names:[...selectedNames].sort(),selected_final_owners:selected,selected_declaration_history:index.history.filter(r=>selectedNames.has(r.name)),source_inputs:[...inputs.values()],ordered_occurrences:occurrences,banking_owner_modified:false,current_pp2_retained:true,canonical_mounted:false,native_proof:false},null,2)+'\n');
console.log(JSON.stringify({root:commit,roots:roots.length,inputs:inputs.size,occurrences:occurrences.length,selected_effective_owners:selected.length,unresolved_native_hold:index.unresolved.length}));
