import fs from'node:fs';import cp from'node:child_process';import path from'node:path';import assert from'node:assert/strict';import {createHash}from'node:crypto';
const prior='a52538b17e234d76ac3cfc5f43165a3bfaa103e1',input='docs/healthroster-build/proposals/a0-source-lineage-selector-successor-20261015/forward/',base='docs/healthroster-build/proposals/a0-source-revision-selector-successor-20261015';
const sha=b=>createHash('sha256').update(b).digest('hex'),add="'SOURCE_GROUP_REPORT_REVISION','SOURCE_GROUP_REVISION_EVENT'";
if(fs.existsSync(base))throw Error('Existing revision packet');const put=(f,b)=>{fs.mkdirSync(path.dirname(base+'/'+f),{recursive:true});fs.writeFileSync(base+'/'+f,b,{flag:'wx'});};const records=[];
function transform(file,edit){const before=cp.execFileSync('git',['cat-file','blob',prior+':'+input+file],{maxBuffer:4e6});let s=before.toString();const hunks=[],once=(label,a,b)=>{assert.equal(s.split(a).length,2,label);s=s.replace(a,()=>b);hunks.push({label,before:a,after:b});};edit(once);let back=s;for(const h of [...hunks].reverse()){assert.equal(back.split(h.after).length,2,h.label);back=back.replace(h.after,()=>h.before);}assert.deepEqual(Buffer.from(back),before);put('before/'+file,before);put('forward/'+file,s);records.push({file,source_commit:prior,source_path:input+file,before_bytes:before.length,before_sha256:sha(before),after_bytes:Buffer.byteLength(s),after_sha256:sha(s),hunks});}
transform('00_source_token_grammar.sqlinc',once=>once('closed native group-conjoined revision selection',"'SOURCE_LINEAGE_EVENT_CLIENT') then","'SOURCE_LINEAGE_EVENT_CLIENT',"+add+') then'));
transform('01_complete_same_arbiter_schema.sqlinc',once=>once('same scalar index native group revision domains',"'SOURCE_LINEAGE_EVENT_CLIENT') and uuid_identity is null","'SOURCE_LINEAGE_EVENT_CLIENT',"+add+') and uuid_identity is null'));
transform('02_complete_scalar_helpers.sqlinc',once=>{
 once('closed new domain set',"'SOURCE_LINEAGE_EVENT_CLIENT')\n      or exists","'SOURCE_LINEAGE_EVENT_CLIENT',"+add+')\n      or exists');
 once('canonical native UUID-pair scalar shape',"'SOURCE_LINEAGE_EVENT_CLIENT')\n        and(jsonb_typeof","'SOURCE_LINEAGE_EVENT_CLIENT',"+add+')\n        and(jsonb_typeof');
 once('native group revision selection stays in dependency phase',"'SOURCE_LINEAGE_EVENT_CLIENT') then 1","'SOURCE_LINEAGE_EVENT_CLIENT',"+add+') then 1');
});
transform('03_coverage_and_compatibility_acquire.sqlinc',once=>once('preserved same acquire dispatch recognises group revision domains',"'SOURCE_LINEAGE_EVENT_CLIENT')) then","'SOURCE_LINEAGE_EVENT_CLIENT',"+add+')) then'));
transform('04_native_source_footprint.sqlinc',once=>once('exact nested revision predicate scopes'," -- Native lineage predicate: selected Client AND (selected resolution OR",` -- Revisions are selected in the actual Source group by the current cycle,
 -- a selected Report scope, or an existing snapshot of a selected event.
 -- Current-cycle identity is a genuine original SOURCE_CYCLE_UUID domain.
 tokens:=tokens||jsonb_build_array(jsonb_build_object('kind','SOURCE_CYCLE_UUID','uuid',p_source_cycle_id::text));
 for field in select * from(values
  ('public.weekly_source_report_scopes','SOURCE_GROUP_REPORT_REVISION'),
  ('public.weekly_work_events','SOURCE_GROUP_REVISION_EVENT')
 ) revision_scope(relation_name,token_kind)
 loop
  select value into strict relation from jsonb_array_elements(witness->'relations')
   where value->>'relation'=field.relation_name;
  select value into strict identity_key from jsonb_array_elements(relation->'keys')
   where value->>'column'='id' and value->>'type'='UUID';
  select coalesce(jsonb_agg(jsonb_build_object('kind',field.token_kind,
   'json',jsonb_build_array((witness->>'source_group_id')::uuid,value::uuid)::text)),'[]'::jsonb) into item
   from jsonb_array_elements_text(identity_key->'values');
  tokens:=tokens||item;
 end loop;
 -- Native lineage predicate: selected Client AND (selected resolution OR`));
const nested=`-- Raw primary/conjunction acquisition in the trigger has already occurred.
-- This subsequent phase reads the actual parent group. Missing RR parents
-- refuse with a retryable error; they cannot silently erase a dependency.
create or replace function private.timesheet_correction_a0_source_revision_tokens_v1(p_relation text,p_row jsonb)
returns jsonb language plpgsql volatile security definer set search_path to 'pg_catalog','pg_temp'
as $source_revision$
declare tokens jsonb:='[]'::jsonb; v_group_id uuid; v_state text; item jsonb; token jsonb;
begin
 if p_relation is null then raise exception 'CORRECTION_A0_SOURCE_RAW_CONTEXT_INVALID' using errcode='55000';end if;
 if p_relation not in('public.weekly_source_final_revisions','public.weekly_source_final_snapshot_lines') then return tokens;end if;
 if jsonb_typeof(p_row) is distinct from 'object' then
  raise exception 'CORRECTION_A0_SOURCE_RAW_CONTEXT_INVALID' using errcode='55000';end if;
 if p_relation='public.weekly_source_final_revisions' then
  if coalesce(p_row->>'state','') not in('CURRENT','PREPARED','SUPERSEDED') then return tokens;end if;
  -- The direct actual cycle UUID was acquired in the preceding raw phase.
  select cycle.source_group_id into strict v_group_id from public.weekly_source_cycles cycle
   where cycle.id=(p_row->>'source_cycle_id')::uuid;
  if p_row->>'report_scope_id' is not null then
   tokens:=tokens||jsonb_build_array(jsonb_build_object('kind','SOURCE_GROUP_REPORT_REVISION',
    'json',jsonb_build_array(v_group_id,(p_row->>'report_scope_id')::uuid)::text));
  end if;
  select coalesce(jsonb_agg(jsonb_build_object('kind','SOURCE_GROUP_REVISION_EVENT',
   'json',jsonb_build_array(v_group_id,snapshot.work_event_id)::text)),'[]'::jsonb) into item
   from public.weekly_source_final_snapshot_lines snapshot where snapshot.final_revision_id=(p_row->>'id')::uuid
    and snapshot.work_event_id is not null;
  tokens:=tokens||item;
 else
  select cycle.source_group_id,revision.state into strict v_group_id,v_state
   from public.weekly_source_final_revisions revision join public.weekly_source_cycles cycle on cycle.id=revision.source_cycle_id
   where revision.id=(p_row->>'final_revision_id')::uuid;
  if v_state in('CURRENT','PREPARED','SUPERSEDED') and p_row->>'work_event_id' is not null then
   tokens:=tokens||jsonb_build_array(jsonb_build_object('kind','SOURCE_GROUP_REVISION_EVENT',
    'json',jsonb_build_array(v_group_id,(p_row->>'work_event_id')::uuid)::text));
  end if;
 end if;
 for token in select value from jsonb_array_elements(tokens) loop
  perform private.timesheet_correction_a0_source_token_assert_v1(token);
 end loop;
 select coalesce(jsonb_agg(value order by value),'[]'::jsonb) into tokens
  from(select distinct value from jsonb_array_elements(tokens)) complete;
 return tokens;
exception when no_data_found or too_many_rows then
 raise exception 'CORRECTION_A0_SOURCE_PARENT_SNAPSHOT_UNAVAILABLE' using errcode='40001';
end;
$source_revision$;
alter function private.timesheet_correction_a0_source_revision_tokens_v1(text,jsonb) owner to postgres;
revoke all on function private.timesheet_correction_a0_source_revision_tokens_v1(text,jsonb) from public,anon,authenticated,service_role;

`;
transform('05_raw_selector_helpers.sqlinc',once=>{
 once('actual current-cycle branch raw UUID before any parent lookup'," if p_relation='public.weekly_source_row_resolutions' and p_row->>'upload_row_id' is not null",` if p_relation='public.weekly_source_final_revisions' and p_row->>'state' in('CURRENT','PREPARED','SUPERSEDED')
  and p_row->>'source_cycle_id' is not null then
  tokens:=tokens||jsonb_build_array(jsonb_build_object('kind','SOURCE_CYCLE_UUID','uuid',(p_row->>'source_cycle_id')::uuid::text));
 end if;
 if p_relation='public.weekly_source_row_resolutions' and p_row->>'upload_row_id' is not null`);
 once('one actual parent phase owner after the raw extractor',"create or replace function private.timesheet_correction_a0_source_reference_before_v1()",nested+'create or replace function private.timesheet_correction_a0_source_reference_before_v1()');
 once('separate native nested phase union',"declare tokens jsonb:='[]'::jsonb;\nbegin\n if tg_table_schema", "declare tokens jsonb:='[]'::jsonb; nested_tokens jsonb:='[]'::jsonb;\nbegin\n if tg_table_schema");
 once('raw OLD/NEW acquire precedes all real parent reads and nested acquire'," perform private.timesheet_correction_a0_fence_acquire_v1(tokens,false);\n if tg_op='DELETE'",` perform private.timesheet_correction_a0_fence_acquire_v1(tokens,false);
 if tg_op in('UPDATE','DELETE') then
  nested_tokens:=nested_tokens||private.timesheet_correction_a0_source_revision_tokens_v1('public.'||tg_table_name,to_jsonb(old));
 end if;
 if tg_op in('INSERT','UPDATE') then
  nested_tokens:=nested_tokens||private.timesheet_correction_a0_source_revision_tokens_v1('public.'||tg_table_name,to_jsonb(new));
 end if;
 perform private.timesheet_correction_a0_fence_acquire_v1(nested_tokens,false);
 if tg_op='DELETE'`);
});
put('COMPLETE_CURRENT_REVERSES.json',JSON.stringify({source_only:true,native:false,records},null,2)+'\n');
const source='fa66183f01913e86651e82816f5e88f97e07123c',contractPath='supabase/release/current-contract.json',b=cp.execFileSync('git',['cat-file','blob',source+':'+contractPath],{maxBuffer:32e6}),c=JSON.parse(b);
const columns=[];for(const [name,names]of [['weekly_source_cycles',['id','source_group_id']],['weekly_source_final_revisions',['id','source_cycle_id','report_scope_id','state']],['weekly_source_final_snapshot_lines',['final_revision_id','work_event_id']]]){const relation=c.relations.find(r=>r.schema==='public'&&r.name===name);for(const column of names){const meta=relation.columns.find(x=>x.name===column);assert.equal(meta.type,column==='state'?'text':'uuid');columns.push({relation:'public.'+name,...meta});}}
put('REVISION_PHYSICAL_COLUMN_EVIDENCE.json',JSON.stringify({source_only:true,native:false,source_commit:source,source_path:contractPath,source_sha256:sha(b),columns,hosted_catalogue_observed:false},null,2)+'\n');
put('SOURCE_BOUNDARY.md','# Nested native revision selector construction\n\nUnreviewed source only. Six complete owners reverse to a525 through eleven unique hunks. The actual bulk reader selects native CURRENT/PREPARED/SUPERSEDED revisions in its Source group through current-cycle OR selected Report-scope OR selected snapshot-event predicates. Two canonical group-conjoined UUID-pair domains and the genuine current Source-cycle UUID represent those arms. Raw OLD/NEW primary/conjunction/current-cycle UUID acquisition precedes a separate actual parent-group read and second same-arbiter acquire. A missing parent in an old snapshot raises retryable40001 and never silently drops scope. No fake parent/group/epoch/family, reader volatility relabel, Source CAS change or second store.\n\nThe parent phase must be qualified with actual visibility, legal foreign-group/state/excluded scopes, source-cycle/revision immutable-owner and concurrent parent/child schedules. Native lock ordering, old-RR and disjoint progression are unproved. Transitive policy/claim dependencies, full raw trigger registration/source order, Agency phase/ACTIVE refusal/recovery and native/registry/verifier/contract/full acceptance remain required. No canonical mount, SQL/lab/lock, fixture, release, external effect or cleanup.\n');
const artifacts=fs.readdirSync(base,{recursive:true}).filter(f=>fs.statSync(base+'/'+f).isFile()).sort().map(file=>{const b=fs.readFileSync(base+'/'+file);return{file:file.replaceAll('\\','/'),bytes:b.length,sha256:sha(b)};});put('SOURCE_MANIFEST.json',JSON.stringify({source_only:true,native:false,mount_ready:false,artifacts},null,2)+'\n');console.log(JSON.stringify({artifacts:artifacts.length,whole_owners:records.length,hunks:records.reduce((n,r)=>n+r.hunks.length,0)}));
