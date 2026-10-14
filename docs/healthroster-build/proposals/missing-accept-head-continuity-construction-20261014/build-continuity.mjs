import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),proposals=path.dirname(dir),sha=b=>createHash('sha256').update(b).digest('hex'),commit='33927fff2f7b90b90241d746d9e35aa21c57bd07';assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));
const pins=[];function git(file){const b=execFileSync('git',['show',commit+':'+file],{cwd:root,maxBuffer:4e6});pins.push({commit,file,bytes:b.length,sha256:sha(b)});return b.toString();}
const ordinary=git('supabase/repeatable/15092026_1534_weekly_source_ordinary_pay_projection_v1.sql'),publisher=git('supabase/repeatable/17092026_0300_weekly_source_entitlement_publication_v1.sql');git('supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql');git('supabase/migrations/04102026_2155_weekly_source_accepted_publication_actions_v1.sql');
const assemblyStart=ordinary.indexOf('    select coalesce(pg_catalog.jsonb_agg(',ordinary.indexOf('create or replace function private.weekly_source_effective_inventory_v1(')),assemblyEnd=ordinary.indexOf('    v_result:=',assemblyStart);const assembly=ordinary.slice(assemblyStart,assemblyEnd);assert.ok(assembly.endsWith('    where component.head_id=v_head.id;\n'));
fs.mkdirSync(path.join(dir,'input'),{recursive:true});fs.writeFileSync(path.join(dir,'input/native_head_components_assembly.sqlinc'),assembly);
const chain=fs.readFileSync(path.join(dir,'forward/00_missing_accept_head_chain.sqlinc'),'utf8');assert.ok(chain.includes(assembly),'Historical component representation is the exact existing native I7 assembly');
const before=fs.readFileSync(path.join(proposals,'missing-accept-current-native-construction-20261014/forward/04_missing_accept_current_removal.sqlinc')),eol=before.includes(Buffer.from('\r\n'))?'\r\n':'\n';let removal=before.toString();const hunks=[];
function hunk(name,old,next){old=old.replaceAll('\n',eol);next=next.replaceAll('\n',eol);assert.equal(removal.split(old).length,2,name);removal=removal.replace(old,next);hunks.push({name,before:old,after:next});}
hunk('actual current and anchor locals','  v_count integer;v_inventory jsonb;',`  v_current_head public.weekly_source_entitlement_heads%rowtype;
  v_chain jsonb;v_current_receipt jsonb;v_current_components jsonb;
  v_count integer;v_inventory jsonb;`);
hunk('current native protected head may retain the genuine accepted removal',"if v_head.state is distinct from 'COMMITTED_CURRENT' or v_head.authority_kind is distinct from 'LOCKED_FINAL_SOURCE'", "if v_head.state is distinct from 'COMMITTED_CURRENT' or v_head.authority_kind not in ('LOCKED_FINAL_SOURCE','PROTECTED')");
hunk('derive genuine linked acceptance before original proof validation',`  select count(*) into v_count from private.weekly_source_accepted_publication_actions_v1 action
    where action.planned_head_id=v_head.id;`,`  v_current_head:=v_head;
  v_chain:=private.weekly_source_missing_accept_head_chain_v1(v_current_head.id,v_event.id);
  if v_chain is null then return null;end if;
  if v_chain->>'current_head_id' is distinct from v_current_head.id::text or v_chain->>'work_event_id' is distinct from v_event.id::text then
    raise exception 'WEEKLY_SOURCE_MISSING_ACCEPT_CURRENT_PROOF_UNBOUND' using errcode='55000';end if;
  select * into strict v_head from public.weekly_source_entitlement_heads where id=(v_chain->>'anchor_head_id')::uuid;
  if v_head.authority_kind is distinct from 'LOCKED_FINAL_SOURCE' then
    raise exception 'WEEKLY_SOURCE_MISSING_ACCEPT_CURRENT_PROOF_UNBOUND' using errcode='55000';end if;
  v_current_receipt:=v_chain#>'{native_head_chain,0,native_receipt}';
  v_current_components:=v_chain#>'{native_head_chain,0,canonical_components}';
  select count(*) into v_count from private.weekly_source_accepted_publication_actions_v1 action
    where action.planned_head_id=v_head.id;`);
hunk('anchor native components still equal actual accepted canonical vector',`  for v_component in select value,ordinality from jsonb_array_elements(v_inventory->'components') with ordinality order by ordinality loop`,`  if (v_chain->'native_head_chain'->(jsonb_array_length(v_chain->'native_head_chain')-1)->'canonical_components')
    is distinct from v_canonical#>'{financial_request,member_entitlements,0,components}' then
    raise exception 'WEEKLY_SOURCE_MISSING_ACCEPT_CURRENT_COMPONENTS_CHANGED' using errcode='55000';end if;
  for v_component in select value,ordinality from jsonb_array_elements(v_inventory->'components') with ordinality order by ordinality loop`);
hunk('current descendant complete components remain independently proved',`  if v_components is distinct from v_canonical#>'{financial_request,member_entitlements,0,components}'
    or v_head.component_count is distinct from jsonb_array_length(v_components)
    or v_head.certified_zero is distinct from (jsonb_array_length(v_components)=0) then`,`  if v_components is distinct from v_current_components
    or v_current_head.component_count is distinct from jsonb_array_length(v_components)
    or v_current_head.certified_zero is distinct from (jsonb_array_length(v_components)=0) then`);
hunk('result head and receipt are actual current publication',`'accepted_source_removed',true,'head',to_jsonb(v_head),'accepted_action',to_jsonb(v_action),
    'native_publication_receipt',to_jsonb(v_receipt)`,`'accepted_source_removed',true,'head',to_jsonb(v_current_head),'accepted_action',to_jsonb(v_action),
    'native_publication_receipt',v_current_receipt`);
hunk('descendant adds full genuine continuity without changing direct result domain',`  return v_result||jsonb_build_object('removal_sha256'`,`  if v_current_head.id is distinct from v_head.id then
    v_result:=v_result||jsonb_build_object('acceptance_head',to_jsonb(v_head),'acceptance_publication_receipt',to_jsonb(v_receipt),
      'native_head_continuity',v_chain);
  end if;
  return v_result||jsonb_build_object('removal_sha256'`);
const after=Buffer.from(removal);for(const h of [...hunks].reverse()){assert.equal(removal.split(h.after).length,2);removal=removal.replace(h.after,h.before);}assert.deepEqual(Buffer.from(removal),before);
fs.mkdirSync(path.join(dir,'before'),{recursive:true});fs.writeFileSync(path.join(dir,'before/01_complete_current_removal.sqlinc'),before);fs.writeFileSync(path.join(dir,'forward/01_complete_current_removal.sqlinc'),after);
fs.writeFileSync(path.join(dir,'COMPLETE_CURRENT_REVERSE.json'),JSON.stringify({source_only:true,own_input_commit:'f1aa8d578b34d2016e916f29c43e5b0c617ed1e8',input:'docs/healthroster-build/proposals/missing-accept-current-native-construction-20261014/forward/04_missing_accept_current_removal.sqlinc',before_bytes:before.length,before_sha256:sha(before),after_bytes:after.length,after_sha256:sha(after),hunks,unique_hunks:hunks.length,whole_reverse_verified:true,canonical_mounted:false,native_proof:false},null,2)+'\n');
fs.writeFileSync(path.join(dir,'CURRENT_INPUT_PINS.json'),JSON.stringify({source_only:true,current_input_commit:commit,inputs:pins,native_head_assembly:{file:'input/native_head_components_assembly.sqlinc',bytes:Buffer.byteLength(assembly),sha256:sha(assembly),byte_exact_native_expression:true},native_publisher_digest_formula_retained:publisher.includes("'components',v_component_hashes"),canonical_mounted:false,native_proof:false},null,2)+'\n');
console.log(JSON.stringify({new_private_signature:'private.weekly_source_missing_accept_head_chain_v1(uuid,uuid)',complete_existing_owner:1,unique_hunks:hunks.length,after_sha256:sha(after),native_assembly_sha256:sha(assembly)}));
