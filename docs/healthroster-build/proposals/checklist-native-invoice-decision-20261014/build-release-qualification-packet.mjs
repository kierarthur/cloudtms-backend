// Shared current native release qualification construction. No SQL execution.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..');
const input='docs/healthroster-build/proposals/checklist-native-invoice-20261014/input/public_weekly_source_invoice_hold_release_atomic_v1.sqlinc';
const before=fs.readFileSync(path.join(repo,input),'utf8'),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const start=before.indexOf('\nbegin\n')+'\nbegin\n'.length,end=before.indexOf('  net:=s.source_shift_charge_pence;');
assert.ok(start>7&&end>start);
const prefix=before.slice(start,end);
const coreName='private.weekly_source_invoice_hold_release_context_core_v1';
const assertName='private.weekly_source_invoice_hold_first_invoice_assert_v1';
const locks=[
 '  select * into strict g from public.weekly_source_groups where weekly_source_groups.id=(select source_group_id from public.weekly_source_cycles where weekly_source_cycles.id=u.source_cycle_id) for update;',
 '  select * into strict c from public.weekly_source_cycles where weekly_source_cycles.id=u.source_cycle_id for update;',
 '    select * into strict scope_row from public.weekly_source_report_scopes where weekly_source_report_scopes.id=u.report_scope_id for update;'];
let corePrefix=prefix;const changes=[];
for(const lock of locks){assert.equal(corePrefix.split(lock).length,2);const indent=lock.match(/^ */)[0],read=lock.replace(' for update;',';');
 const after=`${indent}if p_native_locks then\n${lock}\n${indent}else\n${read}\n${indent}end if;`;
 corePrefix=corePrefix.replace(lock,after);changes.push({before:lock,after});}
const declare=before.slice(before.indexOf('declare actor uuid;'),start-'\nbegin\n'.length);
const header=`create or replace function ${coreName}(p_request jsonb,p_native_locks boolean)
returns jsonb language plpgsql volatile security definer set search_path to 'pg_catalog','pg_temp'
as $release_context$
`;
const frame=`  return jsonb_build_object('actor_user_id',actor,'upload_row_id',row_id,'expected_source_row_hash',encode(expected_hash,'hex'),
    'expected_authority_scope_version',expected_version,'hold_kind',kind,'choice',choice,'row',to_jsonb(s),'upload',to_jsonb(u),
    'group',to_jsonb(g),'cycle',to_jsonb(c),'report_scope',to_jsonb(scope_row),'rules',rules,'client_id',v_client_id);
`;
const core=header+declare+`\nbegin\n  if p_native_locks is null then raise exception 'WEEKLY_SOURCE_INVOICE_HOLD_NATIVE_MODE_INVALID' using errcode='55000'; end if;\n`+corePrefix+frame+`end;
$release_context$;
alter function ${coreName}(jsonb,boolean) owner to postgres;
revoke all on function ${coreName}(jsonb,boolean) from public,anon,authenticated,service_role;
`;
const placementStart=before.indexOf('  if exists(select 1 from public.weekly_source_billing_movements m where');
const placementEnd=before.indexOf("    raise exception 'WEEKLY_SOURCE_INVOICE_HOLD_ALREADY_PLACED' using errcode='55000'; end if;",placementStart)+"    raise exception 'WEEKLY_SOURCE_INVOICE_HOLD_ALREADY_PLACED' using errcode='55000'; end if;".length;
assert.ok(placementStart>end&&placementEnd>placementStart);
const placement=before.slice(placementStart,placementEnd);
const placementCore=`create or replace function ${assertName}(p_upload_row_id uuid)
returns void language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $first_invoice$
begin
${placement.replaceAll('s.id','p_upload_row_id')}
end;
$first_invoice$;
alter function ${assertName}(uuid) owner to postgres;
revoke all on function ${assertName}(uuid) from public,anon,authenticated,service_role;
`;
let wrapper=before;const hunks=[];
function once(name,a,b){assert.equal(wrapper.split(a).length,2,name);wrapper=wrapper.replace(a,()=>b);hunks.push({name,before:a,after:b});}
once('one native context variable','  net bigint; vat bigint; gross bigint; h bytea; id uuid;',
 '  net bigint; vat bigint; gross bigint; h bytea; id uuid; native_context jsonb;');
const call=`  native_context:=${coreName}(p_request,true);
  actor:=(native_context->>'actor_user_id')::uuid;row_id:=(native_context->>'upload_row_id')::uuid;
  expected_hash:=decode(native_context->>'expected_source_row_hash','hex');expected_version:=(native_context->>'expected_authority_scope_version')::bigint;
  kind:=native_context->>'hold_kind';choice:=native_context->>'choice';rules:=native_context->'rules';v_client_id:=(native_context->>'client_id')::uuid;
  s:=jsonb_populate_record(null::public.weekly_source_upload_rows,native_context->'row');
  u:=jsonb_populate_record(null::public.weekly_source_uploads,native_context->'upload');
  g:=jsonb_populate_record(null::public.weekly_source_groups,native_context->'group');
  c:=jsonb_populate_record(null::public.weekly_source_cycles,native_context->'cycle');
  scope_row:=jsonb_populate_record(null::public.weekly_source_report_scopes,native_context->'report_scope');
`;
once('single complete native qualification prefix',prefix,call);
once('same native first-invoice assertion at original post-replay site',placement,`  perform ${assertName}(s.id);`);
let restored=wrapper;for(const h of [...hunks].reverse())restored=restored.replace(h.after,()=>h.before);assert.equal(restored,before);
let originalPrefix=corePrefix;for(const c of [...changes].reverse())originalPrefix=originalPrefix.replace(c.after,()=>c.before);assert.equal(originalPrefix,prefix);
const forward=core+'\n'+placementCore+'\n'+wrapper;
const reverse=before+`\ndrop function ${assertName}(uuid);\ndrop function ${coreName}(jsonb,boolean);\n`;
const save=(file,bytes)=>{const absolute=path.join(repo,file);if(fs.existsSync(absolute)&&!fs.readFileSync(absolute).equals(Buffer.from(bytes)))throw Error('RELEASE_CONSTRUCTION_CHANGED:'+file);fs.writeFileSync(absolute,bytes);};
save(path.relative(repo,path.join(here,'NATIVE_RELEASE_QUALIFICATION.forward.sqlinc')),forward);
save(path.relative(repo,path.join(here,'NATIVE_RELEASE_QUALIFICATION.reverse.sqlinc')),reverse);
save(path.relative(repo,path.join(here,'NATIVE_RELEASE_REVERSE.json')),JSON.stringify({status:'UNMOUNTED_NATIVE_RELEASE_QUALIFICATION_SOURCE_CONSTRUCTION',
 source_commit:'fa66183f01913e86651e82816f5e88f97e07123c',before_path:input,before_sha256:sha(before),prefix_sha256:sha(prefix),
 wrapper_sha256:sha(wrapper),forward_sha256:sha(forward),reverse_sha256:sha(reverse),hunks,lock_sites:changes,
 native:false,true_mode_native_parity:false,false_mode_native_purity:false,Checklist_calls_false_mode:false,
 ordinary_money_unchanged:true,writer_money_tail_unchanged:true,writer_replay_order_unchanged:true,writer_ACL_changed:false},null,2)+'\n');
console.log(JSON.stringify({source_only:true,one_complete_prefix:true,lock_sites:changes.length,whole_reverse_exact:true,native:false}));
