import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {functionEvents} from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),commit='a6739abd7382759ebdbf8a58e7877835258ede9c';
const sourceRoot='docs/healthroster-build/proposals/kept-pay-identical-rekey-current-root-forward-20261014/',sha=b=>createHash('sha256').update(b).digest('hex');
const git=file=>execFileSync('git',['--no-replace-objects','show',commit+':'+file],{cwd:root,maxBuffer:16*1024*1024});
const manifestBytes=git(sourceRoot+'FROZEN_ARTIFACT_MANIFEST.json'),manifest=JSON.parse(manifestBytes);assert.equal(sha(manifestBytes),'3e878999a0b04b24ca0bbd0776bb1f042bc805df142904f28b26c3403dc0aca8');
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')),'Frozen successor cannot be overwritten');fs.mkdirSync(path.join(dir,'before'),{recursive:true});fs.mkdirSync(path.join(dir,'forward'),{recursive:true});const changes=[];
function compose(leaf,hunks){const sourceFile='forward/'+leaf,pin=manifest.artifacts.find(x=>x.file===sourceFile);assert.ok(pin);const bytes=git(sourceRoot+sourceFile);assert.equal(bytes.length,pin.bytes);assert.equal(sha(bytes),pin.sha256);const before=bytes.toString('utf8');let after=before;
 for(const h of hunks){assert.equal(after.split(h.before).length,2,'Unique exact hunk '+leaf);after=after.replace(h.before,h.after);}let reverse=after;for(const h of hunks.toReversed()){assert.equal(reverse.split(h.after).length,2);reverse=reverse.replace(h.after,h.before);}assert.equal(reverse,before);
 assert.deepEqual(functionEvents(after).events.filter(x=>x.kind==='CREATE').map(x=>x.id),functionEvents(before).events.filter(x=>x.kind==='CREATE').map(x=>x.id));
 fs.writeFileSync(path.join(dir,'before',leaf),bytes);fs.writeFileSync(path.join(dir,'forward',leaf),after);changes.push({source_commit:commit,source_file:sourceRoot+sourceFile,source_packet_manifest_sha256:sha(manifestBytes),before_file:'before/'+leaf,forward_file:'forward/'+leaf,before_bytes:bytes.length,before_sha256:sha(bytes),after_bytes:Buffer.byteLength(after),after_sha256:sha(after),hunks,whole_unique_reverse:true,signatures_unchanged:true});}
const addShape="  if jsonb_typeof(p_payload) is distinct from 'object' or jsonb_typeof(p_payload->'add_adjustments') is distinct from 'array' then";
const idsBefore=`  select array_agg(distinct (item->>'credited_work_event_id')::uuid order by (item->>'credited_work_event_id')::uuid)
    into v_ids from jsonb_array_elements(p_payload->'add_adjustments') item where item ? 'credited_work_event_id';`;
const idsAfter=`  select coalesce(array_agg(distinct (item->>'credited_work_event_id')::uuid order by (item->>'credited_work_event_id')::uuid),'{}'::uuid[])
    into v_add_ids from jsonb_array_elements(coalesce(p_payload->'add_adjustments','[]'::jsonb)) item where item ? 'credited_work_event_id';
  -- Capture genuine existing pointer lines BEFORE any own deletion. Removal-only
  -- historical edits need identity/facts scope, not fresh missing-shift eligibility.
  select coalesce(jsonb_agg(jsonb_build_object('invoice_line_id',line.id,'invoice_id',line.invoice_id,
    'work_event_id',(line.meta_json->>'credited_work_event_id')::uuid,
    'metadata_sha256',encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_CREDIT_REMOVAL_METADATA_V1',line.meta_json),'hex'))
    order by line.id),'[]'::jsonb) into v_removal_lines
  from public.invoice_lines line where line.invoice_id=p_invoice_id
    and line.id in(select value::uuid from jsonb_array_elements_text(coalesce(p_payload->'remove_invoice_line_ids','[]'::jsonb)) value)
    and line.meta_json ? 'credited_work_event_id';
  if exists(select 1 from public.invoice_lines line where line.invoice_id=p_invoice_id
    and line.id in(select value::uuid from jsonb_array_elements_text(coalesce(p_payload->'remove_invoice_line_ids','[]'::jsonb)) value)
    and line.meta_json ? 'credited_work_event_id'
    and private.weekly_source_invoice_adjustment_line_valid_v1(line.id,p_invoice_id) is distinct from true) then
    raise exception 'WEEKLY_SOURCE_INVOICE_EDIT_ADJUSTMENT_ONLY' using errcode='22023'; end if;
  select array_agg(distinct id order by id) into v_ids from (
    select unnest(v_add_ids) id union select (item->>'work_event_id')::uuid from jsonb_array_elements(v_removal_lines) item
  ) affected;`;
const eligibility=`      or v_context->'source_present' is distinct from 'false'::jsonb
      or not (v_context->'invoiced'='true'::jsonb or v_context->'paid'='true'::jsonb)
      or v_context->'current_pay_head_movement_id'='null'::jsonb`;
compose('08_credited_targets_admit.sqlinc',[
 {before:'  v_frame jsonb; v_frame_hash bytea;',after:'  v_frame jsonb; v_frame_hash bytea; v_add_ids uuid[]; v_removal_lines jsonb;'},
 {before:addShape,after:`  if jsonb_typeof(p_payload) is distinct from 'object'
    or (p_payload ? 'add_adjustments' and jsonb_typeof(p_payload->'add_adjustments') is distinct from 'array')
    or (p_payload ? 'remove_invoice_line_ids' and jsonb_typeof(p_payload->'remove_invoice_line_ids') is distinct from 'array') then`},
 {before:"  if exists(select 1 from jsonb_array_elements(p_payload->'add_adjustments') item",after:"  if exists(select 1 from jsonb_array_elements(coalesce(p_payload->'add_adjustments','[]'::jsonb)) item"},
 {before:idsBefore,after:idsAfter},
 {before:eligibility,after:`      or (v_id=any(v_add_ids) and (v_context->'source_present' is distinct from 'false'::jsonb
        or not (v_context->'invoiced'='true'::jsonb or v_context->'paid'='true'::jsonb)
        or v_context->'current_pay_head_movement_id'='null'::jsonb))`},
 {before:`      or v_context->'source_present' is distinct from 'false'::jsonb
      or not (v_context->'invoiced'='true'::jsonb or v_context->'paid'='true'::jsonb) then`,after:`      or (v_id=any(v_add_ids) and (v_context->'source_present' is distinct from 'false'::jsonb
        or not (v_context->'invoiced'='true'::jsonb or v_context->'paid'='true'::jsonb))) then`},
 {before:"    'native_contexts',v_after,'adjustment_authority',v_authority);",after:"    'native_contexts',v_after,'add_target_ids',to_jsonb(v_add_ids),'removal_lines',v_removal_lines,'adjustment_authority',v_authority);"}
]);
const hookBefore=`      then p_payload->'add_adjustments' else '[]'::jsonb end) item where item ? 'credited_work_event_id') then`;
const removedHook=`      then p_payload->'add_adjustments' else '[]'::jsonb end) item where item ? 'credited_work_event_id')
    or exists(select 1 from public.invoice_lines removed where removed.invoice_id=p_invoice_id
      and removed.meta_json ? 'credited_work_event_id'
      and removed.id in(select case when pg_input_is_valid(value,'uuid') then value::uuid end
        from jsonb_array_elements_text(case when jsonb_typeof(p_payload->'remove_invoice_line_ids')='array'
          then p_payload->'remove_invoice_line_ids' else '[]'::jsonb end) value))) then`;
// Retain public native ownership and its first original invoice lock; only the
// purpose-bound hook condition expands to actual existing removal targets.
compose('09_complete_invoice_apply_edits.sqlinc',[
 {before:"    and exists(select 1 from jsonb_array_elements(case when jsonb_typeof(p_payload->'add_adjustments')='array'",after:"    and (exists(select 1 from jsonb_array_elements(case when jsonb_typeof(p_payload->'add_adjustments')='array'"},
 {before:hookBefore,after:removedHook}
]);
const writerHookBefore=`    then p_payload->'add_adjustments' else '[]'::jsonb end) requested where requested ? 'credited_work_event_id') then`;
const writerHookAfter=`    then p_payload->'add_adjustments' else '[]'::jsonb end) requested where requested ? 'credited_work_event_id')
    or exists(select 1 from public.invoice_lines removed where removed.invoice_id=p_invoice_id
      and removed.meta_json ? 'credited_work_event_id'
      and removed.id in(select case when pg_input_is_valid(value,'uuid') then value::uuid end
        from jsonb_array_elements_text(case when jsonb_typeof(p_payload->'remove_invoice_line_ids')='array'
          then p_payload->'remove_invoice_line_ids' else '[]'::jsonb end) value)) then`;
const perItem=`        select saved into v_credit_context from jsonb_array_elements(v_credit_frame->'native_contexts') saved
          where (saved->>'work_event_id')::uuid=(item->>'credited_work_event_id')::uuid;
        if v_credit_context is null or v_credit_context->>'facts_fingerprint' is distinct from encode(
          private.weekly_source_kept_as_paid_facts_v1((item->>'credited_work_event_id')::uuid,'CREDITED'),'hex') then
          raise exception 'WEEKLY_SOURCE_CREDITED_SHIFT_FACTS_CHANGED' using errcode='40001'; end if;`;
const beforeRemoval="  if p_payload ? 'remove_invoice_line_ids' then";
const prevalidation=`  -- KC1: validate the complete genuine pre-write census ONCE, after original
  -- invoice authority/lock admission and BEFORE any accepted own removal/add.
  -- The outer Source owner retains all native Candidate/family/group/cycle locks.
  if v_credit_frame is not null then
    select coalesce(array_agg(distinct id order by id),'{}'::uuid[]) into v_target_ids from (
      select (item->>'credited_work_event_id')::uuid id
        from jsonb_array_elements(coalesce(p_payload->'add_adjustments','[]'::jsonb)) item where item ? 'credited_work_event_id'
      union select (removed.meta_json->>'credited_work_event_id')::uuid
        from public.invoice_lines removed where removed.invoice_id=p_invoice_id and removed.meta_json ? 'credited_work_event_id'
          and removed.id in(select value::uuid from jsonb_array_elements_text(coalesce(p_payload->'remove_invoice_line_ids','[]'::jsonb)) value)
    ) affected;
    select coalesce(array_agg((saved->>'work_event_id')::uuid order by (saved->>'work_event_id')::uuid),'{}'::uuid[])
      into v_frame_ids from jsonb_array_elements(v_credit_frame->'native_contexts') saved;
    if v_frame_ids is distinct from v_target_ids then
      raise exception 'WEEKLY_SOURCE_CREDITED_SHIFT_FACTS_CHANGED' using errcode='40001'; end if;
    select coalesce(jsonb_agg(jsonb_build_object('invoice_line_id',removed.id,'invoice_id',removed.invoice_id,
      'work_event_id',(removed.meta_json->>'credited_work_event_id')::uuid,
      'metadata_sha256',encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_CREDIT_REMOVAL_METADATA_V1',removed.meta_json),'hex'))
      order by removed.id),'[]'::jsonb) into v_removal_lines from public.invoice_lines removed
      where removed.invoice_id=p_invoice_id and removed.meta_json ? 'credited_work_event_id'
        and removed.id in(select value::uuid from jsonb_array_elements_text(coalesce(p_payload->'remove_invoice_line_ids','[]'::jsonb)) value);
    if v_removal_lines is distinct from v_credit_frame->'removal_lines' then
      raise exception 'WEEKLY_SOURCE_CREDITED_SHIFT_FACTS_CHANGED' using errcode='40001'; end if;
    for v_credit_context in select value from jsonb_array_elements(v_credit_frame->'native_contexts') loop
      v_current_context:=private.weekly_source_kept_as_paid_context_v1((v_credit_context->>'work_event_id')::uuid,p_actor_user_id);
      if v_credit_context->>'facts_fingerprint' is distinct from encode(private.weekly_source_kept_as_paid_facts_v1(
          (v_credit_context->>'work_event_id')::uuid,'CREDITED'),'hex')
        or v_current_context->>'client_id' is distinct from inv.client_id::text
        or v_current_context->>'source_group_id' is distinct from v_credit_frame->>'source_group_id'
        or v_current_context->>'candidate_id' is distinct from v_credit_context->>'candidate_id'
        or v_current_context->>'source_cycle_id' is distinct from v_credit_context->>'source_cycle_id'
        or v_current_context->'root_timesheet_ids' is distinct from v_credit_context->'root_timesheet_ids'
        or encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_CREDIT_TARGET_NATIVE_PAID_BASIS_V1',
          v_current_context->'native_paid_basis'),'hex') is distinct from v_credit_context->>'native_paid_basis_sha256' then
        raise exception 'WEEKLY_SOURCE_CREDITED_SHIFT_FACTS_CHANGED' using errcode='40001'; end if;
    end loop;
  end if;
${beforeRemoval}`;
compose('10_complete_adjustments_apply.sqlinc',[
 {before:'  v_credit_frame jsonb; v_pg_context text; v_credit_context jsonb;',after:'  v_credit_frame jsonb; v_pg_context text; v_credit_context jsonb;\n  v_current_context jsonb; v_target_ids uuid[]; v_frame_ids uuid[]; v_removal_lines jsonb;'},
 {before:writerHookBefore,after:writerHookAfter},
 {before:beforeRemoval,after:prevalidation},
 {before:perItem,after:`        -- The complete event/facts frame was validated before any own write.
        -- Same-event accepted additions/removals are not foreign CAS drift.`}
]);
compose('12_keep_ack_native_guard.sqlinc',['zzzz_weekly_source_keep_ack_native_guard','zzzz_weekly_source_keep_ack_update_guard','zzzz_weekly_source_keep_ack_delete_guard'].map(name=>({before:'create trigger '+name,after:'create or replace trigger '+name})));
fs.writeFileSync(path.join(dir,'COMPLETE_SUCCESSOR_AND_REVERSES.json'),JSON.stringify({schema_version:'KEEP_CREDIT_BATCH_AND_REPEATABLE_SUCCESSOR_V1',source_only:true,immutable_input_commit:commit,immutable_keep32_manifest_sha256:sha(manifestBytes),findings:['P1-KC1','P1-KC2'],changes,complete_unique_reversals:changes.length,new_private_public_table_signatures:0,original_source39_recorder_three_bundles_tree_exceptions_unchanged:true,shared13_unchanged_and_proposed_not_approved:true,current_root_canonical_sources_modified:false,sql_compiled:false,native_proof:false,full_primary_implementation_complete:false},null,2)+'\n');
console.log(JSON.stringify({source_only:true,findings:['P1-KC1','P1-KC2'],whole_reversals:changes.length,old_freeze_preserved:true,canonical_changed:false,native_proof:false}));
