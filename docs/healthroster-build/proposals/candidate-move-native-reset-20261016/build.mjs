import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),root=process.argv[2];if(!root)throw Error('ROOT_REQUIRED');
const read=p=>fs.readFileSync(p,'utf8').replace(/\r\n/g,'\n');
const replace=(s,a,b)=>{if(s.split(a).length!==2)throw Error('ANCHOR_NOT_EXACT:'+a.slice(0,90));return s.replace(a,b);};
function fn(s,name){const a=s.indexOf('create or replace function '+name+'(');if(a<0)throw Error('FUNCTION_MISSING:'+name);const end=s.indexOf('$function$;',s.indexOf('as $function$',a));return s.slice(a,end+11).trimEnd()+'\n';}
const identity=read(path.resolve(here,'../checklist-v6-contract-correction-20261016/forward/sql/00_identity.sqlinc'));
let current=fn(identity,'private.timesheet_contract_current_placement_v1');
current=replace(current,"     and m.family_root_id=p.family_root_id and m.role=p.role)","     and m.family_root_id=p.family_root_id and m.role=p.role) and not private.timesheet_contract_native_rotation_binding_v1(to_jsonb(p),false)");
// Parentheses keep the native binding as the alternative to the initial Move target.
current=replace(current,"   or not exists(select 1 from private.timesheet_contract_move_members m where m.operation_id=o.id\n     and m.member_key=p.member_key and m.to_timesheet_id=t.timesheet_id and m.to_version=t.version\n     and m.family_root_id=p.family_root_id and m.role=p.role) and not private.timesheet_contract_native_rotation_binding_v1(to_jsonb(p),false)","   or (not exists(select 1 from private.timesheet_contract_move_members m where m.operation_id=o.id\n     and m.member_key=p.member_key and m.to_timesheet_id=t.timesheet_id and m.to_version=t.version\n     and m.family_root_id=p.family_root_id and m.role=p.role) and not private.timesheet_contract_native_rotation_binding_v1(to_jsonb(p),false))");
current=replace(current,'      and a.timesheet_id in(m.from_timesheet_id,m.to_timesheet_id)) then',`      and (a.timesheet_id in(m.from_timesheet_id,m.to_timesheet_id)
        or private.timesheet_contract_native_rotation_alias_v1(to_jsonb(a)))) then`);
fs.writeFileSync(path.join(here,'forward/02_current_placement.sqlinc'),current);
let guard=fn(identity,'private.timesheet_contract_move_placement_guard_v1');
guard=replace(guard," if tg_op='DELETE' then raise exception 'CONTRACT_MOVE_PLACEMENT_IMMUTABLE' using errcode='55000'; end if;",` if tg_op='DELETE' then raise exception 'CONTRACT_MOVE_PLACEMENT_IMMUTABLE' using errcode='55000'; end if;
 if tg_op='UPDATE' and new.operation_id=old.operation_id then
  if (to_jsonb(new)-array['current_timesheet_id','placement_revision']) is distinct from
      (to_jsonb(old)-array['current_timesheet_id','placement_revision'])
    or new.current_timesheet_id=old.current_timesheet_id or new.placement_revision<>old.placement_revision+1
    or not private.timesheet_contract_native_rotation_binding_v1(to_jsonb(new),true) then
   raise exception 'CONTRACT_MOVE_PLACEMENT_CAS_REQUIRED' using errcode='40001';end if;
  return new;
 end if;`);
fs.writeFileSync(path.join(here,'forward/03_placement_guard.sqlinc'),guard);
let rotate=fn(read(path.join(root,'supabase/repeatable/27082026_0423_candidate_electronic_rejection_resubmission_v1.sql')),'private._candidate_timesheet_reject_rotate_v1');
rotate=replace(rotate,'  v_weekly_source_guard jsonb;','  v_weekly_source_guard jsonb;\n  v_move jsonb;\n  v_old_placement jsonb;');
rotate=replace(rotate,'begin\n  select current_row.* into v_current',`begin
  if exists(select 1 from private.timesheet_contract_family_aliases a where a.timesheet_id=p_timesheet_id) then
    v_move:=private.timesheet_contract_move_family_lock_v1(p_timesheet_id);
    if v_move->>'current_timesheet_id' is distinct from p_expected_timesheet_id::text then
      raise exception 'TIMESHEET_MOVED' using errcode='40001';end if;
    select to_jsonb(p) into strict v_old_placement from private.timesheet_contract_current_placements p
      join private.timesheet_contract_family_aliases a on a.family_root_id=p.family_root_id and a.member_key=p.member_key
      where a.timesheet_id=p_timesheet_id;
    if v_old_placement->>'role'<>'WORK' or exists(select 1 from private.timesheet_contract_current_placements sibling
      where sibling.family_root_id=(v_old_placement->>'family_root_id')::uuid and sibling.role='CORRECTION_MEMBER') then
      raise exception 'CANDIDATE_NATIVE_CORRECTION_PAIR_REQUIRED' using errcode='55000';end if;
  end if;
  select current_row.* into v_current`);
// Preserve the native Source protection against the immutable original as
// well as the actual current B. A changed booking never makes Source UNBOUND.
const originalGuardStart=rotate.indexOf('  v_weekly_source_guard:=private.weekly_source_managed_root_guard_v1(');
const originalGuardEnd=rotate.indexOf('  v_new_version:=',originalGuardStart);
if(originalGuardStart<0||originalGuardEnd<0)throw Error('SOURCE_GUARD_NOT_EXACT');
const currentSourceGuard=rotate.slice(originalGuardStart,originalGuardEnd);
const originalSourceGuard=replace(currentSourceGuard,'    v_current.timesheet_id',`    (select a.original_timesheet_id from private.timesheet_contract_family_aliases a where a.timesheet_id=v_current.timesheet_id)`);
rotate=rotate.slice(0,originalGuardStart)+'  if v_old_placement is not null then\n'+originalSourceGuard+'  end if;\n'+rotate.slice(originalGuardStart);
rotate=replace(rotate,'  if v_fin.id is not null then',`  if v_old_placement is not null then
    perform private.timesheet_contract_native_rotation_publish_v1(v_old_placement,v_new_timesheet_id,p_reason,p_actor_user_id,p_now_utc);
  end if;
  if v_fin.id is not null then`);
rotate=replace(rotate,"  perform private._candidate_audit_v1(\n    'timesheet',v_new_timesheet_id::text,'CANDIDATE_ELECTRONIC_REJECTED_VERSION_ROTATED',", "  if v_old_placement is null then\n  perform private._candidate_audit_v1(\n    'timesheet',v_new_timesheet_id::text,'CANDIDATE_ELECTRONIC_REJECTED_VERSION_ROTATED',");
rotate=replace(rotate,'  return v_new_timesheet_id;','  end if;\n  return v_new_timesheet_id;');
fs.writeFileSync(path.join(here,'forward/04_native_rotate.sqlinc'),rotate);
const officeSource=read(path.join(root,'supabase/repeatable/05092026_2100_candidate_submission_delete_reject_guard_v1.sql'));
let targets=fn(officeSource,'private._candidate_office_rejection_targets_v2');
targets=replace(targets,'  v_linked_pending_expense_count integer:=0;','  v_linked_pending_expense_count integer:=0;\n  v_move jsonb;\n  v_candidate_id uuid;');
targets=replace(targets,'  select coalesce(array_agg(workflow_row.id order by workflow_row.id),array[]::uuid[])',`  if exists(select 1 from private.timesheet_contract_family_aliases a where a.timesheet_id=v_timesheet.timesheet_id) then
    select c.candidate_id into strict v_candidate_id from public.contracts c where c.id=v_timesheet.contract_id;
    v_move:=private.candidate_contract_move_context_v1(v_environment,v_candidate_id,v_timesheet.timesheet_id,null);
    if v_move#>>'{dto,current_timesheet_id}' is distinct from v_timesheet.timesheet_id::text then
      raise exception 'TIMESHEET_MOVED' using errcode='40001';end if;
  end if;
  select coalesce(array_agg(workflow_row.id order by workflow_row.id),array[]::uuid[])`);
targets=replace(targets,"      (\n        workflow_row.state not in (",`      (v_move is not null and workflow_row.candidate_id=v_candidate_id
       and workflow_row.id in(select value::uuid from jsonb_array_elements_text(v_move->'workflow_ids'))
       and workflow_row.state not in('REFUSED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED'))
      or (
        workflow_row.state not in (`);
fs.writeFileSync(path.join(here,'forward/05_office_target_workflows.sqlinc'),targets);
let reject=fn(officeSource,'public.candidate_submission_reject_atomic_v1');
const first=reject.indexOf('  -- Serialise every Candidate rejection');const hashStart=reject.indexOf('  v_rejection_request_hash:=');const lockStart=reject.indexOf('  select * into v_timesheet from public.timesheets where timesheet_id=p_timesheet_id for update;');
if(!(first>0&&hashStart>first&&lockStart>hashStart))throw Error('REJECTION_OWNER_ORDER_CHANGED');
const family=reject.slice(first,hashStart),replay=reject.slice(hashStart,lockStart);
reject=reject.slice(0,first)+replay+`  -- Native immutable replay precedes current placement, then family precedes
  -- the shared PAPER advisory and every TS/TF/workflow lock.
  if exists(select 1 from private.timesheet_contract_family_aliases a where a.timesheet_id=p_timesheet_id) then
    perform private.timesheet_contract_move_family_lock_v1(p_timesheet_id);
  end if;
`+family+reject.slice(lockStart);
fs.writeFileSync(path.join(here,'forward/06_office_reject.sqlinc'),reject);
// Native workflows created on an intermediate B retain their genuine immutable
// target. Only exact, certified aliases for this same member enrol them later.
let projection=fn(read(path.resolve(here,'../candidate-contract-move-native-20261016/forward/03_candidate_projection.sqlinc')),'private.candidate_contract_move_context_v1');
projection=replace(projection,'    or v.target_timesheet_id=current_t.timesheet_id or v.anchor_timesheet_id=current_t.timesheet_id);',`    or v.target_timesheet_id=current_t.timesheet_id or v.anchor_timesheet_id=current_t.timesheet_id
    or exists(select 1 from private.timesheet_contract_family_aliases history_alias
      where history_alias.family_root_id=m.family_root_id and history_alias.member_key=m.member_key
       and history_alias.timesheet_id in(v.target_timesheet_id,v.anchor_timesheet_id)
       and (private.timesheet_contract_native_rotation_alias_v1(to_jsonb(history_alias))
        or exists(select 1 from private.timesheet_contract_move_members history_member
          join private.timesheet_contract_move_operations history_operation on history_operation.id=history_member.operation_id
          where history_member.operation_id=history_alias.accepted_operation_id
           and history_operation.state in('COMPLETE','SUPERSEDED') and history_operation.receipt_json is not null
           and history_member.member_key=m.member_key and history_member.family_root_id=m.family_root_id
           and history_alias.member_role=history_member.role
           and history_alias.timesheet_id in(history_member.from_timesheet_id,history_member.to_timesheet_id)))));`);
fs.writeFileSync(path.join(here,'forward/07_candidate_member_workflows.sqlinc'),projection);

// Existing RESUBMIT_REJECTED -> CREATE keeps the immutable original rejection
// identity. Optional reviewed facts are a second additive request-schema repair.
let workflow=read(path.resolve(here,'../candidate-move-paper-draft-20261016/forward/01_native_draft_and_paper_release.sqlinc'));
workflow=replace(workflow,'  v_paper_source_move jsonb;',`  v_paper_source_move jsonb;
  v_resubmit_input jsonb;
  v_resubmit_move jsonb;
  v_resubmit_source jsonb;`);
workflow=replace(workflow,"    v_creation_request_sha256:=private._candidate_workflow_creation_request_sha256_v1(\n      v_creation_request_identity\n    );",`    -- Preserve the exact legacy snapshot-free identity. A supplied snapshot is
    -- sealed as posted; validation follows immutable existing-replacement replay.
    if v_payload ? 'input_snapshot' then
      v_resubmit_input:=v_payload->'input_snapshot';
      v_creation_request_identity:=v_creation_request_identity||jsonb_build_object('input_snapshot',v_resubmit_input);
    end if;
    v_creation_request_sha256:=private._candidate_workflow_creation_request_sha256_v1(
      v_creation_request_identity
    );`);
workflow=replace(workflow,"      and candidate_id=v_candidate_id\n    for update;\n    if not found then\n      raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND'",`      and candidate_id=v_candidate_id;
    if not found then
      raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND'`);
workflow=replace(workflow,'    v_is_rejected_resubmission:=true;',`    if v_resubmit_input is not null then
      if v_source_workflow.scope<>'WEEKLY' or v_source_workflow.workflow_kind='DAILY' then
        raise exception 'CANDIDATE_WORKFLOW_FACTUAL_SNAPSHOT_INVALID' using errcode='22023';end if;
      perform private.candidate_workflow_factual_snapshot_assert_v1(v_resubmit_input);
    end if;
    v_resubmit_source:=to_jsonb(v_source_workflow);
    if v_source_workflow.scope='WEEKLY' then
      v_resubmit_move:=private.candidate_contract_move_context_v1(v_environment,v_candidate_id,null,v_source_workflow.id);
      if v_resubmit_move is not null then
        -- Expense carriers and correction pairs keep their distinct real native
        -- owners. No worked-week target is substituted for a carrier/P0.
        if v_source_workflow.workflow_kind='CONTRACT_EXPENSE'
          or v_source_workflow.rejection_scope='COMPLETE_EXPENSE_CLAIM'
          or v_resubmit_move#>>'{source_member,role}' is distinct from 'WORK'
          or exists(select 1 from private.timesheet_contract_current_placements sibling
            where sibling.family_root_id=(v_resubmit_move#>>'{dto,family_root_id}')::uuid and sibling.role='CORRECTION_MEMBER') then
          raise exception 'CANDIDATE_NATIVE_REPLACEMENT_MEMBER_OWNER_REQUIRED' using errcode='55000';end if;
        perform private.timesheet_contract_move_family_lock_v1((v_resubmit_move#>>'{dto,original_timesheet_id}')::uuid);
        if private.candidate_contract_move_context_v1(v_environment,v_candidate_id,null,v_source_workflow.id)
          is distinct from v_resubmit_move then
          raise exception 'CANDIDATE_CONTEXT_STALE' using errcode='40001';end if;
      end if;
    end if;
    select source_workflow.* into v_source_workflow from public.candidate_submission_workflows source_workflow
      where source_workflow.id=p_workflow_id and source_workflow.environment=v_environment
       and source_workflow.account_id=v_account_id and source_workflow.candidate_id=v_candidate_id for update;
    if not found or to_jsonb(v_source_workflow) is distinct from v_resubmit_source then
      raise exception 'CANDIDATE_CONTEXT_STALE' using errcode='40001';end if;
    v_is_rejected_resubmission:=true;`);
workflow=replace(workflow,"      where current_week.id=v_source_workflow.contract_week_id\n        and current_week.contract_id is not distinct from v_source_workflow.contract_id",`      where current_week.id=case when v_resubmit_move is null then v_source_workflow.contract_week_id
          else (v_resubmit_move->>'current_week_id')::uuid end
        and current_week.contract_id is not distinct from case when v_resubmit_move is null then v_source_workflow.contract_id
          else (select current_t.contract_id from public.timesheets current_t
            where current_t.timesheet_id=(v_resubmit_move#>>'{dto,current_timesheet_id}')::uuid) end`);
workflow=replace(workflow,"        'contract_id',v_source_workflow.contract_id,", "        'contract_id',v_week.contract_id,");
workflow=replace(workflow,"    v_action:='CREATE';\n  end if;",`    if v_resubmit_input is not null then
      v_payload:=v_payload||jsonb_build_object('input_snapshot',v_resubmit_input);
    end if;
    v_action:='CREATE';
  end if;`);
fs.writeFileSync(path.join(here,'forward/08_native_resubmit.sqlinc'),workflow);

let signature=read(path.resolve(here,'../candidate-move-paper-draft-20261016/forward/00_draft_signature_authority.sqlinc'));
signature=replace(signature,"   and w.creation_identity_json#>>'{request,action}'='CREATE'",`   and (w.creation_identity_json#>>'{request,action}'='CREATE'
    or (w.creation_identity_json#>>'{request,action}'='RESUBMIT_REJECTED'
      and w.replacement_of_workflow_id::text=w.creation_identity_json#>>'{request,rejected_workflow_id}'
      and w.creation_identity_json#>>'{derived,replacement_of_workflow_id}'=w.replacement_of_workflow_id::text
      and exists(select 1 from public.candidate_submission_workflows original
        where original.id=w.replacement_of_workflow_id and original.environment=w.environment
         and original.account_id=w.account_id and original.candidate_id=w.candidate_id
         and original.state in('REJECTED','REFUSED')
         and original.generation::text=w.creation_identity_json#>>'{request,rejected_workflow_generation}')))`);
fs.writeFileSync(path.join(here,'forward/09_resubmit_signature.sqlinc'),signature);

const broker=read(path.join(root,'broker/src/candidate-app-backend.js'));
const ja=broker.indexOf('async function handleWorkflowResubmit('),jb=broker.indexOf('\n}\n',ja);
if(ja<0||jb<0)throw Error('RESUBMIT_HANDLER_NOT_FOUND');
let handler=broker.slice(ja,jb+3);
handler=replace(handler,"  await admitCandidateCorrectionWorkflow(request, env, deps, access, workflowId);",`  const payload = {};
  if (Object.prototype.hasOwnProperty.call(body, 'input_snapshot')) {
    payload.input_snapshot = validateCandidateWorkflowFactualSnapshot(body.input_snapshot);
  }
  await admitCandidateCorrectionWorkflow(request, env, deps, access, workflowId);`);
handler=replace(handler,"access, env, workflowId, 'RESUBMIT_REJECTED', generation, {}, idempotencyKey", "access, env, workflowId, 'RESUBMIT_REJECTED', generation, payload, idempotencyKey");
fs.writeFileSync(path.join(here,'forward/10_resubmit_handler.mjsinc'),handler);
console.log('Native rotation and resubmit full owners generated; source review only, native NOT_RUN.');
