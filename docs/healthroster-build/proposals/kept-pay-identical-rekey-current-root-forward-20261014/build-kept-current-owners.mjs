import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {functionEvents} from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const out=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(out,'../../../..');
const current='5db7abee7925fffbeaf7c4e885cce809980fa079';
const sha=b=>createHash('sha256').update(b).digest('hex');
const git=file=>execFileSync('git',['--no-replace-objects','show',current+':'+file],{cwd:root,maxBuffer:32*1024*1024});
const census=JSON.parse(fs.readFileSync(path.join(out,'../current-final-classification-union-20261014/CURRENT_FINAL_DECLARATION_OWNERS.json'),'utf8'));
fs.mkdirSync(path.join(out,'before'),{recursive:true});fs.mkdirSync(path.join(out,'forward'),{recursive:true});
const changes=[],inputs=[];
function owner(id){const pin=census.functions.find(x=>x.id===id);assert.ok(pin,id);const raw=git(pin.file);
 const event=functionEvents(raw.toString('utf8')).events.find(x=>x.kind==='CREATE'&&x.id===id);assert.ok(event);assert.equal(sha(event.definition),pin.definition_sha256,'Current complete owner equal selected checkpoint: '+id);
 inputs.push({id,commit:current,file:pin.file,input_bytes:raw.length,input_sha256:sha(raw),definition_sha256:sha(event.definition),body_sha256:sha(event.body)});return event.definition;}
function compose(id,leaf,hunks){const before=owner(id);let after=before;const actual=[];
 for(const [anchor,addition] of hunks){assert.equal(after.split(anchor).length,2,'One current exact hunk '+id+' '+anchor.slice(0,70));after=after.replace(anchor,addition);actual.push({before:anchor,after:addition});}
 let reverse=after;for(const h of actual.toReversed()){assert.equal(reverse.split(h.after).length,2);reverse=reverse.replace(h.after,h.before);}assert.equal(reverse,before);
 const events=functionEvents(after).events;assert.equal(events.length,1);assert.equal(events[0].id,id);
 fs.writeFileSync(path.join(out,'before',leaf),before+'\n');fs.writeFileSync(path.join(out,'forward',leaf),after+'\n');
 changes.push({signature:id,input_commit:current,before_file:'before/'+leaf,forward_file:'forward/'+leaf,before_sha256:sha(before+'\n'),after_sha256:sha(after+'\n'),hunks:actual,whole_definition_unique_reverse:true});}
const publicHook=`  perform public._ctms_assert_invoice_mutable_draft_v1(`;
compose('public.invoice_apply_edits(uuid,jsonb,uuid)','09_complete_invoice_apply_edits.sqlinc',[[publicHook,
`  -- Source F4 target admission takes complete native I-1 roots BEFORE the
  -- original first invoice mutability/row lock. Ordinary owners are untouched.
  if exists(select 1 from public.invoices invoice where invoice.id=p_invoice_id
    and invoice.header_snapshot_json->>'schema_version'='WEEKLY_SOURCE_SELF_BILL_INVOICE_V1'
    and invoice.header_snapshot_json#>>'{meta,source}'='WEEKLY_FINAL_SOURCE')
    and exists(select 1 from jsonb_array_elements(case when jsonb_typeof(p_payload->'add_adjustments')='array'
      then p_payload->'add_adjustments' else '[]'::jsonb end) item where item ? 'credited_work_event_id') then
    perform private.weekly_source_invoice_credited_targets_admit_v1(p_invoice_id,p_payload,p_actor_user_id);
  end if;
${publicHook}`]]);
const writerId='private.weekly_source_invoice_adjustments_apply_v1(uuid,jsonb,uuid)';
const writerBefore=owner(writerId);inputs.pop();
const refused=`      if item ? 'credited_work_event_id' then\n        raise exception 'WEEKLY_SOURCE_CREDITED_SHIFT_AUTHORITY_UNAVAILABLE' using errcode='55000'; end if;`;
compose(writerId,'10_complete_adjustments_apply.sqlinc',[
 ['  v_ex numeric; v_vat numeric; v_token text; v_description text; v_meta jsonb; v_ids uuid[]; v_id uuid; v_result jsonb;',
  '  v_ex numeric; v_vat numeric; v_token text; v_description text; v_meta jsonb; v_ids uuid[]; v_id uuid; v_result jsonb;\n  v_credit_frame jsonb; v_pg_context text; v_credit_context jsonb;'],
 ['  perform public._ctms_assert_invoice_mutable_draft_v1(p_invoice_id,\'INVOICE_APPLY_EDITS\',true);',
`  if exists(select 1 from jsonb_array_elements(case when jsonb_typeof(p_payload->'add_adjustments')='array'
    then p_payload->'add_adjustments' else '[]'::jsonb end) requested where requested ? 'credited_work_event_id') then
    get diagnostics v_pg_context=PG_CONTEXT;
    v_credit_frame:=nullif(current_setting('cloudtms.weekly_source_credit_target_frame',true),'')::jsonb;
    if v_pg_context !~* 'PL/pgSQL function (public[.])?invoice_apply_edits[(]uuid,jsonb,uuid[)]'
      or jsonb_typeof(v_credit_frame) is distinct from 'object'
      or v_credit_frame->>'schema_version' is distinct from 'WEEKLY_SOURCE_CREDIT_TARGET_FRAME_V1'
      or v_credit_frame->>'invoice_id' is distinct from p_invoice_id::text
      or v_credit_frame->>'actor_user_id' is distinct from p_actor_user_id::text
      or v_credit_frame->>'native_backend_pid' is distinct from pg_backend_pid()::text
      or v_credit_frame->>'native_transaction_id' is distinct from txid_current()::text
      or v_credit_frame->>'payload_sha256' is distinct from encode(private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_CREDIT_TARGET_PAYLOAD_V1',p_payload),'hex')
      or v_credit_frame->>'frame_sha256' is distinct from encode(private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_CREDIT_TARGET_FRAME_V1',v_credit_frame-'frame_sha256'),'hex') then
      raise exception 'WEEKLY_SOURCE_CREDITED_SHIFT_NATIVE_FRAME_REQUIRED' using errcode='55000'; end if;
  end if;
  perform public._ctms_assert_invoice_mutable_draft_v1(p_invoice_id,'INVOICE_APPLY_EDITS',true);`],
 ['  if v_authority->>\'allowed\' is distinct from \'true\' then',
`  if v_credit_frame is not null and v_credit_frame->'adjustment_authority' is distinct from v_authority then
    raise exception 'WEEKLY_SOURCE_CREDITED_SHIFT_FACTS_CHANGED' using errcode='40001'; end if;
  if v_authority->>'allowed' is distinct from 'true' then`],
 [refused,
`      if item ? 'credited_work_event_id' then
        if jsonb_typeof(item->'credited_work_event_id') is distinct from 'string'
          or pg_input_is_valid(item->>'credited_work_event_id','uuid') is distinct from true then
          raise exception 'WEEKLY_SOURCE_CREDITED_SHIFT_INVALID' using errcode='22023'; end if;
        select saved into v_credit_context from jsonb_array_elements(v_credit_frame->'native_contexts') saved
          where (saved->>'work_event_id')::uuid=(item->>'credited_work_event_id')::uuid;
        if v_credit_context is null or v_credit_context->>'facts_fingerprint' is distinct from encode(
          private.weekly_source_kept_as_paid_facts_v1((item->>'credited_work_event_id')::uuid,'CREDITED'),'hex') then
          raise exception 'WEEKLY_SOURCE_CREDITED_SHIFT_FACTS_CHANGED' using errcode='40001'; end if;
      end if;`],
 ["        'vat_overridden',item ? 'vat_amount','weekly_source_adjustment_authority',v_authority);",
 "        'vat_overridden',item ? 'vat_amount','weekly_source_adjustment_authority',v_authority)\n        ||case when item ? 'credited_work_event_id' then jsonb_build_object('credited_work_event_id',(item->>'credited_work_event_id')::uuid) else '{}'::jsonb end;"]
]);
compose('private.weekly_source_invoice_adjustment_line_valid_v1(uuid,uuid)','11_complete_adjustment_line_valid.sqlinc',[
 ["    or line.meta_json ? 'credited_work_event_id' then return false; end if;",'    then return false; end if;'],
 ["    'vat_overridden','weekly_source_adjustment_authority')) then return false; end if;",
 "    'vat_overridden','weekly_source_adjustment_authority','credited_work_event_id')) then return false; end if;"],
 ['  return coalesce(v_actual->>\'allowed\'=\'true\' and v_actual=v_authority',
`  -- A retained issued credit re-proves immutable actual target identity, not
  -- today's Keep availability. Changed facts cannot erase a genuine credit.
  if v_meta ? 'credited_work_event_id' and (jsonb_typeof(v_meta->'credited_work_event_id') is distinct from 'string'
    or pg_input_is_valid(v_meta->>'credited_work_event_id','uuid') is distinct from true
    or not exists(select 1 from public.weekly_work_events event
      join public.invoices invoice on invoice.id=p_invoice_id and invoice.client_id=event.client_id
        and invoice.header_snapshot_json#>>'{meta,source_group_id}'=event.first_source_group_id::text
      where event.id=(v_meta->>'credited_work_event_id')::uuid
        and exists(select 1 from public.weekly_source_row_resolutions resolution
          join public.contracts contract on contract.id=resolution.contract_id
          where resolution.work_event_id=event.id and resolution.mapping_state='RESOLVED'
            and resolution.candidate_id=event.candidate_id and resolution.client_id=event.client_id
            and contract.candidate_id=event.candidate_id and contract.client_id=event.client_id))) then return false; end if;
  return coalesce(v_actual->>'allowed'='true' and v_actual=v_authority`]
]);
const guard=owner('private._weekly_source_immutable_record_guard_v1()');
const schemaBody=fs.readFileSync(path.join(out,'forward/04_kept_as_paid_decisions_schema.sqlinc'),'utf8');
fs.writeFileSync(path.join(out,'forward/00_complete_one_time_kept_decisions.sqlinc'),guard+'\n'
  +'alter function private._weekly_source_immutable_record_guard_v1() owner to postgres;\n'
  +'revoke all on function private._weekly_source_immutable_record_guard_v1() from public,anon,authenticated,service_role;\n'+schemaBody);
const routeFile='broker/src/weekly-source/routes.js',routeRaw=git(routeFile),routeBefore=routeRaw.toString('utf8');
assert.equal(sha(routeRaw),'425e7fefb430e7db09c777ebf852f0e9aaa02ce17e0ca144eaaf2d58dd7c29a5','Published current Undo route input');
const routeAnchor="  'OPEN_MANUAL_REVIEW', 'RESOLVE_MANUAL_REVIEW', 'PREPARE_PROTECTED_EDITOR',";
const routeNew=routeAnchor+"\n  'KEEP_AS_PAID',";
const branchAnchor="  if (action === 'LATER_CHANGE_DECISION') {";
const branch=`  if (action === 'KEEP_AS_PAID') {
    const keys = ['schema_version', 'work_event_id', 'decision', 'reason',
      'credit_invoice_line_id', 'expected_facts_fingerprint', 'idempotency_key'];
    const sourceUuid = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
    if (body.action !== action || Object.keys(body).length !== 2 || !Object.hasOwn(body, 'payload')
        || Object.keys(payload).length !== keys.length || !keys.every((key) => Object.hasOwn(payload, key))
        || payload.schema_version !== 'WEEKLY_SOURCE_KEEP_AS_PAID_REQUEST_V1'
        || typeof payload.work_event_id !== 'string' || !sourceUuid.test(payload.work_event_id)
        || payload.decision !== 'KEEP_AS_PAID' || !['CREDITED', 'NOT_INVOICED_DISAPPEARED'].includes(payload.reason)
        || (payload.reason === 'CREDITED' ? typeof payload.credit_invoice_line_id !== 'string'
          || !sourceUuid.test(payload.credit_invoice_line_id) : payload.credit_invoice_line_id !== null)
        || typeof payload.expected_facts_fingerprint !== 'string' || !/^[0-9a-f]{64}$/.test(payload.expected_facts_fingerprint)
        || typeof payload.idempotency_key !== 'string' || payload.idempotency_key !== payload.idempotency_key.trim()
        || payload.idempotency_key.length < 16 || payload.idempotency_key.length > 200) {
      fail('WEEKLY_SOURCE_KEEP_AS_PAID_REQUEST_INVALID', 'This decision could not be prepared.', 400);
    }
    const result = await withRefusalRecording(dependencies,
      'broker:weekly-source:weekly_source_keep_as_paid_decide_atomic_v1', user.id,
      () => callRpc(dependencies, 'weekly_source_keep_as_paid_decide_atomic_v1', request));
    return jsonResponse(200, result);
  }

${branchAnchor}`;
assert.equal(routeBefore.split(routeAnchor).length,2);assert.equal(routeBefore.split(branchAnchor).length,2);
const routeAfter=routeBefore.replace(routeAnchor,routeNew).replace(branchAnchor,branch);
assert.equal(routeAfter.replace(branch,branchAnchor).replace(routeNew,routeAnchor),routeBefore);
fs.writeFileSync(path.join(out,'before/routes.js'),routeRaw);fs.writeFileSync(path.join(out,'forward/routes.js'),routeAfter);
changes.push({runtime_file:routeFile,input_commit:current,before_file:'before/routes.js',forward_file:'forward/routes.js',before_sha256:sha(routeRaw),after_sha256:sha(routeAfter),hunks:[{before:routeAnchor,after:routeNew},{before:branchAnchor,after:branch}],whole_definition_unique_reverse:true});
const auxiliaryFiles=['supabase/repeatable/05102026_1112_bpay_next_source_paid_evidence_page_v1.sql',
 'supabase/repeatable/05102026_1242_weekly_source_office_next_paid_context_v1.sql',
 'supabase/repeatable/21072026_1235_00b_import_correction_runtime_guards.sql',
 'broker/src/payment-actor-permission.js',routeFile];
const auxiliary=auxiliaryFiles.map(file=>{const bytes=git(file);return{commit:current,file,bytes:bytes.length,sha256:sha(bytes)};});
fs.writeFileSync(path.join(out,'KEPT_CREDIT_CURRENT_OWNERS_AND_INPUT_PINS.json'),JSON.stringify({source_only:true,unfinished_construction:true,current_selected_commit:current,inputs,changes,auxiliary,complete_owner_unique_reversals:changes.length,
 bank_owned_sources_changed:false,bank_public_page_permissions_unchanged:true,source_next_nonadmin_pure_kernel:'PRECODE_SHARED_OWNER_REVIEW_REQUIRED',sql_compiled:false,native_proof:false,all_remaining_original_pipeline_owners:'NOT_YET_COMPLETED_OR_FROZEN'},null,2)+'\n');
console.log(JSON.stringify({source_only:true,unfinished:true,current,complete_sql_owners:changes.filter(x=>x.signature).length,runtime_owner:1,full_reversals:changes.length,bank_owned_sources_changed:false}));
