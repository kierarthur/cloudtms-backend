import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..');
const sha=b=>createHash('sha256').update(b).digest('hex');
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));
for(const f of ['before','forward','input'])fs.mkdirSync(path.join(dir,f),{recursive:true});
const commit='19efb87a26158e53fb4023676aa9b19e117b0685';
const source='docs/healthroster-build/proposals/paid-match-legacy-native-consumer-construction-20261014/forward/09_complete_retained_transition_assert.sqlinc';
const raw=fs.readFileSync(path.join(root,source));
assert.deepEqual(raw,execFileSync('git',['show',commit+':'+source],{cwd:root,maxBuffer:3e6}));
const before=raw.toString(),eol=before.includes('\r\n')?'\r\n':'\n';
const nl=s=>s.replaceAll('\n',eol);
const hunks=[];
function hunk(name,b,a){assert.equal(before.split(b).length,2,name);hunks.push({name,before:b,after:a});}
hunk('finite saved native root and Source selection proof locals',
  '  proof jsonb;item jsonb;rows jsonb;expected jsonb;counted integer;movement_id uuid;lineage_id uuid;economic_id uuid;',
  '  proof jsonb;item jsonb;rows jsonb;expected jsonb;counted integer;movement_id uuid;lineage_id uuid;economic_id uuid;'+eol+
  '  native_basis jsonb;legacy_current jsonb;native_roots uuid[];source_group_id uuid;');
const map=nl(`      or not exists(select 1 from private.bpay_next_source_current_snapshots mapping
        join private.bpay_next_source_current_scopes scope on scope.id=mapping.scope_id
        where mapping.snapshot_line_id=old_line.id and mapping.movement_id=m.id and mapping.work_event_id=t.work_event_id
          and scope.source_group_id=(select source_group_id from public.weekly_source_cycles where id=r.source_cycle_id)
          and (scope.candidate_id,scope.client_id,scope.contract_id)=(old_line.candidate_id,old_line.client_id,old_line.contract_id)) then
      raise exception 'WEEKLY_SOURCE_PAID_MATCH_PRIOR_MOVEMENT_UNBOUND' using errcode='55000';end if;`);
const replacement=nl(`      then
      raise exception 'WEEKLY_SOURCE_PAID_MATCH_PRIOR_MOVEMENT_UNBOUND' using errcode='55000';end if;
    if item#>>'{native_payment_facts,paid_evidence_state}'='SETTLED_ORIGINAL' then
      -- Keep the existing NEXT map predicate unchanged for NEXT proof.
      if not exists(select 1 from private.bpay_next_source_current_snapshots mapping
        join private.bpay_next_source_current_scopes scope on scope.id=mapping.scope_id
        where mapping.snapshot_line_id=old_line.id and mapping.movement_id=m.id and mapping.work_event_id=t.work_event_id
          and scope.source_group_id=(select source_group_id from public.weekly_source_cycles where id=r.source_cycle_id)
          and (scope.candidate_id,scope.client_id,scope.contract_id)=(old_line.candidate_id,old_line.client_id,old_line.contract_id)) then
        raise exception 'WEEKLY_SOURCE_PAID_MATCH_PRIOR_MOVEMENT_UNBOUND' using errcode='55000';end if;
    else
      -- LEGACY publish_revision returns before NEXT capture. Re-prove the
      -- genuine Source prior selection instead of manufacturing a NEXT map.
      select c.source_group_id into strict source_group_id from public.weekly_source_cycles c where c.id=r.source_cycle_id;
      if item#>>'{native_payment_facts,paid_evidence_state}' is distinct from 'SETTLED_LEGACY_SOURCE_COMPONENT'
        or (select control.active_owner from private.bpay_next_module_control control where control.id=1) is distinct from 'LEGACY'
        or item#>>'{native_payment_facts,schema_version}' is distinct from 'WEEKLY_SOURCE_WORK_EVENT_PAYMENT_FACTS_V1'
        or item#>>'{native_payment_facts,work_event_id}' is distinct from t.work_event_id::text
        or item#>>'{native_payment_facts,source_cycle_id}' is distinct from r.source_cycle_id::text
        or item#>>'{native_payment_facts,source_group_id}' is distinct from source_group_id::text
        or item#>>'{native_payment_facts,candidate_id}' is distinct from old_line.candidate_id::text
        or item#>>'{native_payment_facts,client_id}' is distinct from old_line.client_id::text
        or jsonb_typeof(item#>'{native_payment_facts,root_timesheet_ids}') is distinct from 'array'
        or jsonb_array_length(item#>'{native_payment_facts,root_timesheet_ids}') not between 1 and 200
        or jsonb_typeof(item#>'{native_payment_facts,native_paid_basis}') is distinct from 'array'
        or jsonb_array_length(item#>'{native_payment_facts,native_paid_basis}')
          is distinct from jsonb_array_length(item#>'{native_payment_facts,root_timesheet_ids}') then
        raise exception 'WEEKLY_SOURCE_PAID_MATCH_LEGACY_BASIS_UNBOUND' using errcode='55000';end if;
      native_roots:=array(select value::uuid from jsonb_array_elements_text(item#>'{native_payment_facts,root_timesheet_ids}') value order by value);
      if cardinality(native_roots) is distinct from (select count(distinct root) from unnest(native_roots) root) then
        raise exception 'WEEKLY_SOURCE_PAID_MATCH_LEGACY_BASIS_UNBOUND' using errcode='55000';end if;
      for native_basis in select value from jsonb_array_elements(item#>'{native_payment_facts,native_paid_basis}') value loop
        if native_basis->>'schema_version' is distinct from 'WEEKLY_SOURCE_LEGACY_SIGNED_SOURCE_MEMBERSHIP_INPUT_V1'
          or native_basis->>'inputs_sha256' is distinct from encode(private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_SOURCE_LEGACY_SIGNED_SOURCE_MEMBERSHIP_INPUT_V1',native_basis-'inputs_sha256'),'hex')
          or native_basis->>'work_event_id' is distinct from t.work_event_id::text
          or native_basis->>'source_cycle_id' is distinct from r.source_cycle_id::text
          or native_basis->>'source_group_id' is distinct from source_group_id::text
          or native_basis->>'candidate_id' is distinct from old_line.candidate_id::text
          or native_basis->>'client_id' is distinct from old_line.client_id::text
          or native_basis->>'actor_user_id' is distinct from r.finalised_by_user_id::text
          or native_basis#>>'{native_module,active_owner}' is distinct from 'LEGACY'
          or not ((native_basis->>'root_timesheet_id')::uuid=any(native_roots)) then
          raise exception 'WEEKLY_SOURCE_PAID_MATCH_LEGACY_BASIS_UNBOUND' using errcode='55000';end if;
      end loop;
      if array(select (value->>'root_timesheet_id')::uuid
          from jsonb_array_elements(item#>'{native_payment_facts,native_paid_basis}') value order by value->>'root_timesheet_id')
        is distinct from native_roots then
        raise exception 'WEEKLY_SOURCE_PAID_MATCH_LEGACY_BASIS_UNBOUND' using errcode='55000';end if;
      legacy_current:=private.weekly_source_paid_overlap_current_snapshot_for_cycle_v1(t.work_event_id,m.id,
        old_line.candidate_id,old_line.client_id,old_line.contract_id,source_group_id,native_roots,r.source_cycle_id);
      if legacy_current is distinct from item->'current_pay_snapshot'
        or legacy_current->>'state' is distinct from 'CURRENT'
        or legacy_current->>'snapshot_line_id' is distinct from old_line.id::text
        or legacy_current->>'movement_id' is distinct from m.id::text then
        raise exception 'WEEKLY_SOURCE_PAID_MATCH_PRIOR_MOVEMENT_UNBOUND' using errcode='55000';end if;
    end if;`);
hunk('native evidence-specific provenance with unchanged NEXT map and genuine Legacy Source selection',map,replacement);
let after=before;
for(const h of hunks){assert.equal(after.split(h.before).length,2);after=after.replace(h.before,()=>h.after);}
let reverse=after;
for(const h of [...hunks].reverse()){assert.equal(reverse.split(h.after).length,2);reverse=reverse.replace(h.after,()=>h.before);}
assert.equal(reverse,before);
fs.writeFileSync(path.join(dir,'before/01_complete_retained_transition_assert.sqlinc'),raw);
fs.writeFileSync(path.join(dir,'forward/01_complete_retained_transition_assert.sqlinc'),after);
const currentCommit='fa66183f01913e86651e82816f5e88f97e07123c';
const nativeFile='supabase/repeatable/04102026_0505_banking_pay_next_source_current_hooks_v1.sql';
const hooks=execFileSync('git',['show',currentCommit+':'+nativeFile],{cwd:root,maxBuffer:3e6});
fs.writeFileSync(path.join(dir,'input/04102026_0505_current_hooks.sql'),hooks);
const dependencies=[];
for(const f of ['00_roster_prior_states.sqlinc','02_legacy_signed_source_membership.sqlinc','03_current_snapshot_for_cycle.sqlinc','04_complete_payment_facts.sqlinc']){
 const file='docs/healthroster-build/proposals/paid-match-legacy-native-consumer-construction-20261014/forward/'+f;
 const bytes=fs.readFileSync(path.join(root,file));assert.deepEqual(bytes,execFileSync('git',['show',commit+':'+file],{cwd:root,maxBuffer:3e6}));
 dependencies.push({commit,file,bytes:bytes.length,sha256:sha(bytes)});
}
fs.writeFileSync(path.join(dir,'COMPLETE_REVERSE_AND_INPUT_PINS.json'),JSON.stringify({source_only:true,commit,source,before_sha256:sha(raw),after_sha256:sha(after),hunks,whole_reverse_byte_exact:true,native_input:{commit:currentCommit,file:nativeFile,saved:'input/04102026_0505_current_hooks.sql',bytes:hooks.length,sha256:sha(hooks)},dependencies},null,2)+'\n');
console.log(JSON.stringify({before:sha(raw),after:sha(after),hunks:hunks.length}));
