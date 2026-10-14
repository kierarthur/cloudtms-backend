import fs from 'node:fs';import cp from 'node:child_process';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';
const prior='aad0dd848253abca76a4917765b8d20c4d128646';
const base='docs/healthroster-build/proposals/a0-source-lineage-selector-successor-20261015';
const tuple='docs/healthroster-build/proposals/a0-source-selector-tuples-forward-20261014/forward/';
const footprint='docs/healthroster-build/proposals/a0-source-native-footprint-construction-20261015/construction/01_native_source_footprint.sqlinc';
const raw='docs/healthroster-build/proposals/a0-source-raw-selector-guard-construction-20261015/construction/01_raw_selector_helpers.sqlinc';
const sha=b=>createHash('sha256').update(b).digest('hex'),add="'SOURCE_LINEAGE_RESOLUTION_CLIENT','SOURCE_LINEAGE_EVENT_CLIENT'";
if(fs.existsSync(base))throw Error('Existing immutable lineage packet');const put=(f,b)=>{fs.mkdirSync(path.dirname(base+'/'+f),{recursive:true});fs.writeFileSync(base+'/'+f,b,{flag:'wx'});};const records=[];
function transform(file,source,edit){const before=cp.execFileSync('git',['cat-file','blob',prior+':'+source],{maxBuffer:4e6});let s=before.toString();const hunks=[],once=(label,a,b)=>{assert.equal(s.split(a).length,2,label);s=s.replace(a,()=>b);hunks.push({label,before:a,after:b});};edit(once);let back=s;for(const h of [...hunks].reverse()){assert.equal(back.split(h.after).length,2,h.label);back=back.replace(h.after,()=>h.before);}assert.deepEqual(Buffer.from(back),before);put('before/'+file,before);put('forward/'+file,s);records.push({file,source_commit:prior,source_path:source,before_bytes:before.length,before_sha256:sha(before),after_bytes:Buffer.byteLength(s),after_sha256:sha(s),hunks});}
transform('00_source_token_grammar.sqlinc',tuple+'00_source_token_grammar.sqlinc',once=>once('closed real lineage conjunction domains',"'SOURCE_REVISION_EVENT','SOURCE_RESOLUTION_SLOT') then","'SOURCE_REVISION_EVENT','SOURCE_RESOLUTION_SLOT',"+add+') then'));
transform('01_complete_same_arbiter_schema.sqlinc',tuple+'01_complete_same_arbiter_schema.sqlinc',once=>once('same scalar arbiter accepts native lineage pairs',"'SOURCE_REVISION_EVENT','SOURCE_RESOLUTION_SLOT') and uuid_identity is null","'SOURCE_REVISION_EVENT','SOURCE_RESOLUTION_SLOT',"+add+') and uuid_identity is null'));
transform('02_complete_scalar_helpers.sqlinc',tuple+'02_complete_scalar_helpers.sqlinc',once=>{
 once('closed scalar validation',"'SOURCE_REVISION_EVENT','SOURCE_RESOLUTION_SLOT')\n      or exists","'SOURCE_REVISION_EVENT','SOURCE_RESOLUTION_SLOT',"+add+')\n      or exists');
 once('UUID pair identities are canonical scalar JSONB text',"'SOURCE_REVISION_EVENT','SOURCE_RESOLUTION_SLOT')\n        and(jsonb_typeof","'SOURCE_REVISION_EVENT','SOURCE_RESOLUTION_SLOT',"+add+')\n        and(jsonb_typeof');
 once('lineage conjunctions stay in dependency phase',"'SOURCE_REVISION_EVENT','SOURCE_RESOLUTION_SLOT') then 1","'SOURCE_REVISION_EVENT','SOURCE_RESOLUTION_SLOT',"+add+') then 1');
});
transform('03_coverage_and_compatibility_acquire.sqlinc',tuple+'03_coverage_and_compatibility_acquire.sqlinc',once=>once('preserved original acquire dispatch',"'SOURCE_REVISION_EVENT','SOURCE_RESOLUTION_SLOT')) then","'SOURCE_REVISION_EVENT','SOURCE_RESOLUTION_SLOT',"+add+')) then'));
transform('04_native_source_footprint.sqlinc',footprint,once=>once('complete native lineage OR of two Client-conjoined scopes'," if jsonb_typeof(selectors->'group_client_pairs') is distinct from 'array'",` -- Native lineage predicate: selected Client AND (selected resolution OR
 -- selected event). Both phantom arms remain distinct typed conjunctions.
 for field in select * from(values
  ('public.weekly_source_row_resolutions','SOURCE_LINEAGE_RESOLUTION_CLIENT'),
  ('public.weekly_work_events','SOURCE_LINEAGE_EVENT_CLIENT')
 ) lineage(relation_name,token_kind)
 loop
  select value into strict relation from jsonb_array_elements(witness->'relations')
   where value->>'relation'=field.relation_name;
  select value into strict identity_key from jsonb_array_elements(relation->'keys')
   where value->>'column'='id' and value->>'type'='UUID';
  select coalesce(jsonb_agg(jsonb_build_object('kind',field.token_kind,
   'json',jsonb_build_array(parent_id::uuid,client_id::uuid)::text)),'[]'::jsonb) into item
   from jsonb_array_elements_text(identity_key->'values') parents(parent_id)
   cross join jsonb_array_elements_text(clients) selected(client_id);
  tokens:=tokens||item;
 end loop;
 if jsonb_typeof(selectors->'group_client_pairs') is distinct from 'array'`));
transform('05_raw_selector_helpers.sqlinc',raw,once=>once('raw lineage resolution/event Client pairs before any parent lookup'," if p_relation='public.contracts' and p_row->>'client_id' is not null",` if p_relation='public.weekly_source_row_timesheet_lineages' and p_row->>'client_id' is not null then
  if p_row->>'row_resolution_id' is not null then
   tokens:=tokens||jsonb_build_array(jsonb_build_object('kind','SOURCE_LINEAGE_RESOLUTION_CLIENT',
    'json',jsonb_build_array((p_row->>'row_resolution_id')::uuid,(p_row->>'client_id')::uuid)::text));
  end if;
  if p_row->>'work_event_id' is not null then
   tokens:=tokens||jsonb_build_array(jsonb_build_object('kind','SOURCE_LINEAGE_EVENT_CLIENT',
    'json',jsonb_build_array((p_row->>'work_event_id')::uuid,(p_row->>'client_id')::uuid)::text));
  end if;
 end if;
 if p_relation='public.contracts' and p_row->>'client_id' is not null`));
put('COMPLETE_CURRENT_REVERSES.json',JSON.stringify({source_only:true,native:false,records},null,2)+'\n');
const source='fa66183f01913e86651e82816f5e88f97e07123c',contractPath='supabase/release/current-contract.json';const contractRaw=cp.execFileSync('git',['cat-file','blob',source+':'+contractPath],{maxBuffer:32e6}),contract=JSON.parse(contractRaw);const relation=contract.relations.find(r=>r.schema==='public'&&r.name==='weekly_source_row_timesheet_lineages');const columns=['row_resolution_id','work_event_id','client_id'].map(name=>{const c=relation.columns.find(c=>c.name===name);assert.equal(c.type,'uuid');return c;});
put('LINEAGE_PHYSICAL_COLUMN_EVIDENCE.json',JSON.stringify({source_only:true,native:false,source_commit:source,source_path:contractPath,source_sha256:sha(contractRaw),relation:'public.weekly_source_row_timesheet_lineages',columns,hosted_catalogue_observed:false},null,2)+'\n');
put('SOURCE_BOUNDARY.md','# Exact native lineage phantom conjunctions\n\nUnreviewed source-only successor to aad0. Six complete owners reverse exactly through eight unique hunks. Native lineage selection is selected Client AND (selected resolution OR selected event). The constructor retains full native selected-resolution/Client and event/Client products in two distinct same-arbiter typed UUID-pair domains. Raw OLD/NEW lineages emit each genuine pair only when its Client and parent UUID exist. No parent lookup, broad resolution/event token, fake family or new store is introduced. Existing primary-object and physical-family guards remain.\n\nNested revision predicates, transitive policy/claim dependencies, full source trigger registration/order, actual Agency phase/CAS and durable ACTIVE refusal/recovery, native locking/dirty/RR/security/contract and final acceptance remain required. No canonical mount, SQL/lab/lock, fixture, release, external effect or cleanup.\n');
const artifacts=fs.readdirSync(base,{recursive:true}).filter(f=>fs.statSync(base+'/'+f).isFile()).sort().map(file=>{const b=fs.readFileSync(base+'/'+file);return{file:file.replaceAll('\\','/'),bytes:b.length,sha256:sha(b)};});put('SOURCE_MANIFEST.json',JSON.stringify({source_only:true,native:false,mount_ready:false,artifacts},null,2)+'\n');console.log(JSON.stringify({artifacts:artifacts.length,whole_owners:records.length,hunks:records.reduce((n,r)=>n+r.hunks.length,0)}));
