import fs from 'node:fs';import path from 'node:path';import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';import {fileURLToPath} from 'node:url';import assert from 'node:assert/strict';
import {functionEvents,applyFunctionEvents,emptyFunctionIndex} from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
import {scanManagedSql} from '../strict-qualification-install1-forward/PINNED_MANAGED_SQL_SCANNER.mjs';
import {sqlDateKey} from '../strict-qualification-install1-forward/PINNED_SQL_DATE_ORDER.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),commit='fa66183f01913e86651e82816f5e88f97e07123c';
const sha=b=>createHash('sha256').update(b).digest('hex');assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));
const git=(args,input)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],{cwd:root,input,maxBuffer:40e6});
const files=git(['ls-tree','-r','--name-only',commit,'--','supabase']).toString().trim().split('\n').filter(f=>/\.(sql|sqlinc|inc)$/i.test(f)||f==='supabase/release/current-release.json');
const cache=new Map();for(let i=0;i<files.length;i+=64){const part=files.slice(i,i+64),b=git(['cat-file','--batch'],part.map(f=>`${commit}:${f}\n`).join(''));let cursor=0;
 for(const f of part){const end=b.indexOf(10,cursor),h=b.subarray(cursor,end).toString().split(' ');assert.equal(h[1],'blob');const n=Number(h[2]);cursor=end+1;cache.set(f,b.subarray(cursor,cursor+n));cursor+=n+1;}assert.equal(cursor,b.length);}
assert.deepEqual(cache.get('scripts/cloudtms-managed-release-sql.mjs')??git(['show',commit+':scripts/cloudtms-managed-release-sql.mjs']),fs.readFileSync(path.join(dir,'../strict-qualification-install1-forward/PINNED_MANAGED_SQL_SCANNER.mjs')));
const release=JSON.parse(cache.get('supabase/release/current-release.json')),chronological=rows=>rows.sort((a,b)=>sqlDateKey(a).localeCompare(sqlDateKey(b))||a.localeCompare(b));
const roots=[...release.baselineFiles,...chronological(files.filter(f=>/^supabase\/migrations\/.*\.sql$/i.test(f))),...chronological(files.filter(f=>/^supabase\/repeatable\/.*\.sql$/i.test(f)))];
const index=emptyFunctionIndex(),inputs=new Map(),occurrences=[];let order=0;
function visit(file,stack=[]){assert.ok(!stack.includes(file));assert.ok(cache.has(file),file);const b=cache.get(file),s=b.toString().replaceAll('\r\n','\n'),scan=scanManagedSql(s);
 inputs.set(file,{file,bytes:b.length,sha256:sha(b)});occurrences.push({file,include_stack:stack,sha256:sha(b)});
 for(const u of [...scan.statements.map(r=>({...r,kind:'statement'})),...scan.meta.map(r=>({...r,kind:'meta'}))].sort((a,b)=>a.start-b.start)){
 const body=s.slice(u.start,u.end);if(u.kind==='meta'){const m=body.match(/^\s*\\ir\s+(?:'([^']+)'|"([^"]+)"|([^\s;]+))/);if(m)visit(path.posix.normalize(path.posix.join(path.posix.dirname(file),m[1]??m[2]??m[3])),[...stack,file]);continue;}
 const p=functionEvents(body);index.unresolved.push(...p.unresolved.map(r=>({...r,file,order:order++})));
 applyFunctionEvents(index,p.events,e=>({...e,file,include_stack:stack,order:order++,source_file_sha256:sha(b),definition_sha256:sha(e.definition??body)}));}}
for(const file of roots)visit(file);
const containing=JSON.parse(fs.readFileSync(path.join(dir,'input/body_amendment_census.json'))),expected=new Map(containing.sourceInputs.map(r=>[r.file,r]));
assert.equal(roots.length,1484);assert.equal(inputs.size,1745);assert.equal(occurrences.length,2160);
for(const [file,p] of inputs){const e=expected.get(file);assert.ok(e,file);assert.equal(p.sha256,e.sha256);assert.equal(p.bytes,e.bytes);}
const names=new Set(['public.weekly_source_office_workspace_v1','private.weekly_source_office_authority_v1','private.weekly_source_pay_actor_allowed_v1',
 'private.weekly_source_query_current_publication_v1','private.weekly_source_query_require_service_v1','public.weekly_source_office_recheck_begin_v1',
 'public.weekly_source_projection_publish_atomic_v1','public.weekly_source_upload_context_v1','private.weekly_source_protected_match_candidates_v1']);
const selected=[...index.live.values()].filter(r=>names.has(r.name)).map(({definition,body,...r})=>({...r,body_sha256:sha(body)}));
const workspace=selected.find(r=>r.id==='public.weekly_source_office_workspace_v1(jsonb)');assert.ok(workspace);assert.equal(workspace.file,'supabase/repeatable/14102026_0044_weekly_source_protected_pay_authority_v2.sql');
const before=fs.readFileSync(path.join(dir,'before/02_complete_office_workspace.sqlinc'),'utf8');assert.equal(sha(before.replaceAll('\r\n','\n')),workspace.definition_sha256);
const directInputs=[...new Set(selected.map(r=>r.file))].map(f=>inputs.get(f));
fs.writeFileSync(path.join(dir,'CURRENT_DECLARATION_AND_BODY_PATCH_PINS.json'),JSON.stringify({source_only:true,commit,casefold_sql_inventory:true,root_count:roots.length,input_paths:inputs.size,occurrences:occurrences.length,
 all_1745_input_hashes_equal_corrected_root_case_census:true,ordered_source_occurrences_sha256:sha(JSON.stringify(occurrences)),complete_literal_semantic_symbols:index.live.size,
 unresolved_dynamic_statements:index.unresolved.length,selected_final_literal_declarations:selected,direct_source_input_pins:directInputs,
 guarded_pg_get_functiondef_read_sites:containing.amendments.length,
 guarded_or_catalogue_amendments_remain_native_and_whole_body_reconstruction_holds:true,
 supplied_containing_body_census:'973625f17cd51a49803872a819638e10f13bbbff',
 source_PPV2_is_controlling_literal_workspace_owner:true,not_a_final_effective_catalogue_or_native_proof:true},null,2)+'\n');
console.log(JSON.stringify({roots:roots.length,inputs:inputs.size,occurrences:occurrences.length,selected: selected.length,direct_source_files:directInputs.length,symbols:index.live.size,native:false}));
