import fs from 'node:fs';import path from 'node:path';import {execFileSync} from 'node:child_process';import {createHash} from 'node:crypto';import {fileURLToPath} from 'node:url';import assert from 'node:assert/strict';
import {functionEvents,applyFunctionEvents,emptyFunctionIndex} from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
import {scanManagedSql} from '../strict-qualification-install1-forward/PINNED_MANAGED_SQL_SCANNER.mjs';import {sqlDateKey} from '../strict-qualification-install1-forward/PINNED_SQL_DATE_ORDER.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),commit='fa66183f01913e86651e82816f5e88f97e07123c',sha=b=>createHash('sha256').update(b).digest('hex');
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));
const git=(args,input)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],{cwd:root,input,maxBuffer:40e6});
const files=git(['ls-tree','-r','--name-only',commit,'--','supabase']).toString().trim().split('\n').filter(f=>/\.(sql|sqlinc|inc)$/i.test(f)||f==='supabase/release/current-release.json'),cache=new Map();
for(let i=0;i<files.length;i+=64){const p=files.slice(i,i+64),b=git(['cat-file','--batch'],p.map(f=>`${commit}:${f}\n`).join(''));let cursor=0;for(const f of p){const end=b.indexOf(10,cursor),h=b.subarray(cursor,end).toString().split(' ');assert.equal(h[1],'blob');const n=Number(h[2]);cursor=end+1;cache.set(f,b.subarray(cursor,cursor+n));cursor+=n+1;}assert.equal(cursor,b.length);}
const release=JSON.parse(cache.get('supabase/release/current-release.json')),sort=rows=>rows.sort((a,b)=>sqlDateKey(a).localeCompare(sqlDateKey(b))||a.localeCompare(b));
const roots=[...release.baselineFiles,...sort(files.filter(f=>/^supabase\/migrations\/.*\.sql$/i.test(f))),...sort(files.filter(f=>/^supabase\/repeatable\/.*\.sql$/i.test(f)))],index=emptyFunctionIndex(),inputs=new Map(),occurrences=[];let order=0;
function visit(file,stack=[]){assert.ok(!stack.includes(file));const b=cache.get(file);assert.ok(b,file);const s=b.toString().replaceAll('\r\n','\n'),scan=scanManagedSql(s);inputs.set(file,{file,bytes:b.length,sha256:sha(b)});occurrences.push({file,include_stack:stack,sha256:sha(b)});
 for(const u of [...scan.statements.map(r=>({...r,kind:'statement'})),...scan.meta.map(r=>({...r,kind:'meta'}))].sort((a,b)=>a.start-b.start)){const body=s.slice(u.start,u.end);
 if(u.kind==='meta'){const m=body.match(/^\s*\\ir\s+(?:'([^']+)'|"([^"]+)"|([^\s;]+))/);if(m)visit(path.posix.normalize(path.posix.join(path.posix.dirname(file),m[1]??m[2]??m[3])),[...stack,file]);continue;}
 const p=functionEvents(body);index.unresolved.push(...p.unresolved.map(r=>({...r,file,order:order++})));applyFunctionEvents(index,p.events,e=>({...e,file,include_stack:stack,order:order++,source_file_sha256:sha(b),definition_sha256:sha(e.definition??body)}));}}
for(const f of roots)visit(f);
const censusFile='docs/healthroster-build/proposals/root-effective-body-amendment-census-case-forward-20261014/BODY_AMENDMENT_CENSUS.json';
const cb=git(['show','973625f17cd51a49803872a819638e10f13bbbff:'+censusFile]),census=JSON.parse(cb),expected=new Map(census.sourceInputs.map(r=>[r.file,r]));
assert.deepEqual([roots.length,inputs.size,occurrences.length],[1484,1745,2160]);for(const [file,p] of inputs){assert.equal(p.sha256,expected.get(file)?.sha256);assert.equal(p.bytes,expected.get(file)?.bytes);}
const names=new Set(['private.bpay_next_workbench_refresh_work_core_v1','private.bpay_next_workbench_work_members_v1','private.bpay_next_source_query_segment_exclusions_v1',
 'private.weekly_source_pay_query_gate_v1','private.weekly_source_pay_query_facts_v1','private.weekly_source_pay_query_scope_v1','private.weekly_source_first_authorise_core_v1',
 'public.weekly_source_first_authorise_atomic_v1','private.bpay_next_workbench_kind_page_v1','public.bpay_next_workbench_page_v1','private.bpay_next_workbench_require_next_v1',
 'private.bpay_next_workbench_totals_v1','private.bpay_next_workbench_refresh_group_display_v1','private.bpay_next_workbench_group_contribution_v1',
 'private.bpay_next_workbench_dirty_group_v1','private.bpay_next_workbench_mark_dirty_v1','private.bpay_next_workbench_ensure_metadata_v1',
 'private.bpay_next_workbench_metadata_v1','private.bpay_next_workbench_require_service_v1','private.bpay_next_workbench_display_kind_page_v1',
 'private.bpay_next_create_preparation_v1','private.bpay_next_selection_segments_page_v1','private.bpay_next_seal_selected_segments_v1',
 'private.bpay_next_capture_components_page_v1','private.weekly_source_invoice_duty_v1']);
fs.mkdirSync(path.join(dir,'input'),{recursive:true});const selected=[];
for(const r of [...index.live.values()].filter(r=>names.has(r.name))){const {definition,body,...meta}=r;
 const arity=r.id.slice(r.id.indexOf('(')+1,-1).split(',').filter(Boolean).length;
 const suffix=r.name==='private.bpay_next_create_preparation_v1'?`__${arity}_args`:'';
 const f=`input/${r.name.replace('.','_')}${suffix}.sqlinc`;fs.writeFileSync(path.join(dir,f),definition);
 selected.push({...meta,body_sha256:sha(body),saved:f});}
const schemaFiles=['supabase/migrations/10102026_1252_banking_pay_next_workbench_summary.sql','supabase/migrations/10102026_1946_banking_pay_next_candidate_group_display.sql',
 'supabase/repeatable/12102026_2146_banking_pay_next_treatment_workbench_query_identity_v1.sql','supabase/repeatable/12102026_2146_banking_pay_next_treatment_workbench_members_v1.sql',
 'supabase/migrations/04102026_0300_banking_pay_next_case_instruction_core.sql','supabase/repeatable/13102026_0016_banking_pay_next_accepted_eligible_leaf_v1.sql'];
const schemaPins=[];for(const file of schemaFiles){const b=cache.get(file);assert.ok(b);const saved='input/'+path.basename(file);fs.writeFileSync(path.join(dir,saved),b);schemaPins.push({file,saved,bytes:b.length,sha256:sha(b)});}
fs.writeFileSync(path.join(dir,'CURRENT_NATIVE_SOURCE_PINS.json'),JSON.stringify({source_only:true,commit,casefold_SQL:true,roots:roots.length,input_paths:inputs.size,include_occurrences:occurrences.length,
 complete_literal_semantic_symbols:index.live.size,unresolved_dynamic_statements:index.unresolved.length,corrected_body_census_containing_commit:'973625f17cd51a49803872a819638e10f13bbbff',
 corrected_body_census_file:censusFile,corrected_body_census_sha256:sha(cb),all_input_hashes_matched:true,ordered_occurrences_sha256:sha(JSON.stringify(occurrences)),
 selected_literal_owners:selected,direct_source_file_pins:[...new Set(selected.map(r=>r.file))].map(f=>inputs.get(f)),schema_and_selected_native_raw_pins:schemaPins,
 absent_selected_names:[...names].filter(n=>!selected.some(r=>r.name===n)),guarded_pg_get_functiondef_read_sites:census.amendments.length,
 final_effective_current_body_and_native_catalogue_proof:false,native_calls:0,Banking_owner_modified:false,not_implemented_adapter:true},null,2)+'\n');
console.log(JSON.stringify({selected:selected.map(r=>({signature:r.id,file:r.file,sha256:r.definition_sha256})),roots:roots.length,inputs:inputs.size,occurrences:occurrences.length}));
