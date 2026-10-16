import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),root=process.argv[2];if(!root)throw Error('ROOT_REQUIRED');
const read=p=>fs.readFileSync(p,'utf8').replace(/\r\n/g,'\n'),own=p=>read(path.resolve(here,p));
const replace=(s,a,b)=>{if(s.split(a).length!==2)throw Error('ANCHOR_NOT_EXACT:'+a.slice(0,100));return s.replace(a,()=>b);};
function fn(s,n){const a=s.indexOf('create or replace function '+n+'('),b=s.indexOf('$function$;',s.indexOf('as $function$',a));if(a<0||b<0)throw Error('MISSING:'+n);return s.slice(a,b+11)+'\n';}
let current=own('../candidate-move-native-reset-20261016/forward/02_current_placement.sqlinc');
current=replace(current,'  select v.* into t from public.timesheets v where v.timesheet_id=p.current_timesheet_id;',`  if p.current_timesheet_id is null then
    if private.timesheet_contract_native_pending_v1(to_jsonb(p),false) is null
      or a.member_role is distinct from p.role
      or not exists(select 1 from private.timesheet_contract_move_members member
        join private.timesheet_contract_move_operations accepted on accepted.id=member.operation_id
         and accepted.state in('COMPLETE','SUPERSEDED') and accepted.receipt_json is not null
        where member.operation_id=a.accepted_operation_id and member.member_key=a.member_key
         and member.family_root_id=a.family_root_id and member.role=a.member_role
         and (a.timesheet_id in(member.from_timesheet_id,member.to_timesheet_id)
           or private.timesheet_contract_native_rotation_alias_v1(to_jsonb(a))
           or private.timesheet_contract_native_allocation_alias_v1(to_jsonb(a)))) then
      raise exception 'CURRENT_PLACEMENT_UNBOUND' using errcode='55000';end if;
    return jsonb_build_object('version','CURRENT_CONTRACT_PLACEMENT_V1','requested_timesheet_id',p_timesheet_id,
      'family_root_id',p.family_root_id,'current_timesheet_id',null,'current_contract_id',p.current_contract_id,
      'placement_revision',p.placement_revision::text,'placement_receipt_id',p.placement_receipt_id,'legacy_origin',false);
  end if;
  select v.* into t from public.timesheets v where v.timesheet_id=p.current_timesheet_id;`);
current=replace(current,'and not private.timesheet_contract_native_rotation_binding_v1(to_jsonb(p),false))','and not private.timesheet_contract_native_rotation_binding_v1(to_jsonb(p),false)\n     and not private.timesheet_contract_native_allocation_binding_v1(to_jsonb(p),false)\n     and not private.timesheet_contract_native_allocation_binding_v1(to_jsonb(p),true))');
current=replace(current,'or private.timesheet_contract_native_rotation_alias_v1(to_jsonb(a)))) then','or private.timesheet_contract_native_rotation_alias_v1(to_jsonb(a))\n        or private.timesheet_contract_native_allocation_alias_v1(to_jsonb(a)))) then');
fs.writeFileSync(path.join(here,'forward/04_current_placement.sqlinc'),current);
let gate=fn(own('../checklist-v6-contract-correction-20261016/forward/sql/00_identity.sqlinc'),'private.timesheet_contract_move_family_lock_v1');
gate=replace(gate,'declare placement jsonb; checked jsonb; candidate uuid; root_id uuid;','declare placement jsonb; checked jsonb; candidate uuid; root_id uuid; pending jsonb;');
gate=replace(gate,` select c.candidate_id into candidate from public.timesheets t join public.contracts c on c.id=t.contract_id
  where t.timesheet_id=(placement->>'current_timesheet_id')::uuid;`,` if placement->'current_timesheet_id'='null'::jsonb then
  select private.timesheet_contract_native_pending_v1(to_jsonb(p),false) into pending
   from private.timesheet_contract_current_placements p where p.family_root_id=root_id
    and p.member_key=(select a.member_key from private.timesheet_contract_family_aliases a where a.timesheet_id=p_timesheet_id);
  select c.candidate_id into candidate from public.contracts c where c.id=(placement->>'current_contract_id')::uuid
   and c.candidate_id=(pending#>>'{placement,candidate_id}')::uuid;
 else
  select c.candidate_id into candidate from public.timesheets t join public.contracts c on c.id=t.contract_id
   where t.timesheet_id=(placement->>'current_timesheet_id')::uuid;
 end if;`);
gate=replace(gate,' checked:=private.timesheet_contract_current_placement_v1(p_timesheet_id);',` if pending is not null then perform 1 from public.contract_weeks cw where cw.id=(pending->>'contract_week_id')::uuid for update;end if;
 checked:=private.timesheet_contract_current_placement_v1(p_timesheet_id);`);
fs.writeFileSync(path.join(here,'forward/05_family_lock.sqlinc'),gate);
let guard=own('../candidate-move-native-reset-20261016/forward/03_placement_guard.sqlinc');
guard=replace(guard,'or not private.timesheet_contract_native_rotation_binding_v1(to_jsonb(new),true) then',`or (not private.timesheet_contract_native_rotation_binding_v1(to_jsonb(new),true)
      and private.timesheet_contract_native_pending_v1(to_jsonb(new),true) is null
      and not private.timesheet_contract_native_allocation_binding_v1(to_jsonb(new),true)) then`);
// NULL comparisons require exact IS NOT DISTINCT FROM equality. A true native
// pending transition cannot pass merely because SQL OR evaluated UNKNOWN.
guard=replace(guard,'new.current_timesheet_id=old.current_timesheet_id','new.current_timesheet_id is not distinct from old.current_timesheet_id');
fs.writeFileSync(path.join(here,'forward/06_placement_guard.sqlinc'),guard);
let withdrawal=fn(read(path.join(root,'supabase/repeatable/27082026_2004_candidate_weekly_withdrawal_null_target_v1.sql')),'private._candidate_weekly_withdrawal_reset_v1');
withdrawal=replace(withdrawal,'  v_weekly_source_guard jsonb;','  v_weekly_source_guard jsonb;\n  v_move jsonb;\n  v_old_placement jsonb;\n  v_original_workflow jsonb;');
withdrawal=replace(withdrawal,'  select workflow_row.* into v_workflow',`  select workflow_row.* into v_workflow from public.candidate_submission_workflows workflow_row where workflow_row.id=p_workflow_id;
  if found and v_workflow.scope='WEEKLY' then
    v_move:=private.candidate_contract_move_context_v1(v_workflow.environment,v_workflow.candidate_id,null,v_workflow.id);
    if v_move is not null then
      v_original_workflow:=to_jsonb(v_workflow);
      perform private.timesheet_contract_move_family_lock_v1((v_move#>>'{dto,original_timesheet_id}')::uuid);
      if private.candidate_contract_move_context_v1(v_workflow.environment,v_workflow.candidate_id,null,v_workflow.id)
        is distinct from v_move then raise exception 'CANDIDATE_CONTEXT_STALE' using errcode='40001';end if;
      select to_jsonb(p) into strict v_old_placement from private.timesheet_contract_current_placements p
        where p.family_root_id=(v_move#>>'{dto,family_root_id}')::uuid and p.member_key=v_move#>>'{source_member,member_key}';
      if v_old_placement->>'role' is distinct from 'WORK' or exists(select 1 from private.timesheet_contract_current_placements sibling
        where sibling.family_root_id=(v_old_placement->>'family_root_id')::uuid and sibling.role='CORRECTION_MEMBER') then
        raise exception 'CANDIDATE_NATIVE_CORRECTION_PAIR_REQUIRED' using errcode='55000';end if;
    end if;
  end if;
  select workflow_row.* into v_workflow`);
withdrawal=replace(withdrawal,"  select week_row.* into v_week",`  if v_original_workflow is not null and to_jsonb(v_workflow) is distinct from v_original_workflow then
    raise exception 'CANDIDATE_CONTEXT_STALE' using errcode='40001';end if;
  select week_row.* into v_week`);
withdrawal=replace(withdrawal,`  where week_row.id=v_workflow.contract_week_id
    and week_row.contract_id=v_workflow.contract_id`,`  where week_row.id=case when v_move is null then v_workflow.contract_week_id else (v_move->>'current_week_id')::uuid end
    and week_row.contract_id=case when v_move is null then v_workflow.contract_id else (v_old_placement->>'current_contract_id')::uuid end`);
withdrawal=replace(withdrawal,`  v_source_timesheet_id:=coalesce(
    v_workflow.target_timesheet_id,
    v_workflow.anchor_timesheet_id,
    v_week.timesheet_id
  );`,`  v_source_timesheet_id:=case when v_move is not null then (v_old_placement->>'current_timesheet_id')::uuid else coalesce(
    v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id,v_week.timesheet_id) end;`);
const a=withdrawal.indexOf('    v_weekly_source_guard:='),z=withdrawal.indexOf('    update public.timesheets set',a);if(a<0||z<a)throw Error('WITHDRAWAL_SOURCE_GUARD_MISSING');
const sg=withdrawal.slice(a,z),og=replace(sg,'      v_current.timesheet_id',`      (v_move#>>'{dto,original_timesheet_id}')::uuid`);
withdrawal=withdrawal.slice(0,a)+'    if v_move is not null then\n'+og+'    end if;\n'+withdrawal.slice(a);
withdrawal=replace(withdrawal,"  perform private._candidate_audit_v1(\n    case when v_has_source",`  if v_old_placement is not null then
    perform private.timesheet_contract_native_pending_publish_v1(v_old_placement,v_workflow.id,v_week.id,p_reason,v_system_actor_user_id,p_now_utc);
  else
  perform private._candidate_audit_v1(
    case when v_has_source`);
withdrawal=replace(withdrawal,"  return jsonb_build_object(\n    'reset',true,","  end if;\n  return jsonb_build_object(\n    'reset',true,");
fs.writeFileSync(path.join(here,'forward/07_native_withdrawal.sqlinc'),withdrawal);
let context=own('../candidate-move-native-reset-20261016/forward/07_candidate_member_workflows.sqlinc');
context=replace(context,' placement:=private.timesheet_contract_current_placement_v1(requested_id);',` placement:=private.timesheet_contract_current_placement_v1(requested_id);
 -- The public17 schema requires real current UUIDs. A genuine native pending
 -- Contract Week uses its existing native view with this optional fragment absent.
 if placement->'current_timesheet_id'='null'::jsonb then return null;end if;`);
fs.writeFileSync(path.join(here,'forward/08_candidate_context.sqlinc'),context);

// Complete actual current shared Manual allocator. Only finite native pending
// admission and callbacks are added; all original finance/Source/evidence owners
// and final CREATED_PROCESSED remain the real complete authority.
const manualSource=read(path.join(root,'supabase/repeatable/10102026_0942_weekly_source_manual_upsert_busy_passthrough_v1.sql'));
const ma=manualSource.indexOf('CREATE OR REPLACE FUNCTION public.contract_week_manual_upsert_atomic('),mz=manualSource.indexOf('$function$;',ma);
if(ma<0||mz<0)throw Error('ACTUAL_MANUAL_OWNER_MISSING');let manual=manualSource.slice(ma,mz+11)+'\n';
manual=replace(manual,'  v_weekly_source_guard jsonb;','  v_weekly_source_guard jsonb;\n  v_native_pending jsonb;\n  v_native_allocation_audit_id uuid;');
manual=replace(manual,'  SELECT cw.*\n    INTO v_week',`  -- Existing native pending identity is resolved/relocked before CW/TS/TF.
  v_native_pending:=private.timesheet_contract_native_pending_enter_week_v1(p_week_id);
  SELECT cw.*
    INTO v_week`);
manual=replace(manual,'  v_tsfin_result := public.tsfin_write_current_snapshot_single_bounded(',`  if v_native_pending is not null then
    v_native_allocation_audit_id:=private.timesheet_contract_native_allocation_publish_v1(
      v_native_pending,v_current_ts.timesheet_id,v_week.id,v_created_now,p_actor_user_id,v_now);
  end if;
  v_tsfin_result := public.tsfin_write_current_snapshot_single_bounded(`);
manual=replace(manual,`      'primary_timesheet_storage_key', v_primary_timesheet_storage_key
    ),
    'WEEKLY_MANUAL_PROCESS',`,`      'primary_timesheet_storage_key', v_primary_timesheet_storage_key
    )||case when v_native_allocation_audit_id is null then '{}'::jsonb else
      jsonb_build_object('native_allocation_audit_id',v_native_allocation_audit_id) end,
    'WEEKLY_MANUAL_PROCESS',`);
const auditEnd=`    'WEEKLY_MANUAL_PROCESS',
    p_actor_user_id
  );`;
manual=replace(manual,auditEnd,auditEnd+`
  if v_native_pending is not null and not exists(select 1 from private.timesheet_contract_current_placements p
    where p.current_timesheet_id=v_current_ts.timesheet_id and private.timesheet_contract_native_allocation_binding_v1(to_jsonb(p),false)) then
    raise exception 'CURRENT_PLACEMENT_NATIVE_ALLOCATION_NOT_PROCESSED' using errcode='23514';end if;`);
fs.writeFileSync(path.join(here,'forward/09_native_manual_allocation.sqlinc'),manual);

let rotations=own('../candidate-move-qr-paper-20261016/forward/00_native_rotation_bindings.sqlinc');
rotations=replace(rotations,'new_t.booking_id=m.current_booking_id',`(new_t.booking_id=m.current_booking_id or private.timesheet_contract_native_booking_bound_v1(o.id,m.member_key,new_t.booking_id,new_t.contract_id,new_t.week_ending_date))`);
rotations=replace(rotations,'and m.current_booking_id=t.booking_id and m.to_contract_id=t.contract_id',`and (m.current_booking_id=t.booking_id or private.timesheet_contract_native_booking_bound_v1(o.id,m.member_key,t.booking_id,t.contract_id,t.week_ending_date)) and m.to_contract_id=t.contract_id`);
fs.writeFileSync(path.join(here,'forward/03c_rotation_bindings.sqlinc'),rotations);
for(const [input,output] of [
 ['../candidate-move-native-reset-20261016/forward/01_native_rotation_publish.sqlinc','03d_electronic_rotation_publish.sqlinc'],
 ['../candidate-move-qr-paper-20261016/forward/01_native_qr_publish.sqlinc','03e_qr_rotation_publish.sqlinc']]){
 let publisher=own(input);
 publisher=replace(publisher,'or new_t.booking_id is distinct from old_t.booking_id or new_t.booking_id is distinct from m.current_booking_id',`or new_t.booking_id is distinct from old_t.booking_id
  or (new_t.booking_id is distinct from m.current_booking_id and not private.timesheet_contract_native_booking_bound_v1(o.id,m.member_key,new_t.booking_id,new_t.contract_id,new_t.week_ending_date))`);
 fs.writeFileSync(path.join(here,'forward/'+output),publisher);
}
context=replace(context,'(private.timesheet_contract_native_rotation_alias_v1(to_jsonb(history_alias))',`(private.timesheet_contract_native_rotation_alias_v1(to_jsonb(history_alias))
        or private.timesheet_contract_native_allocation_alias_v1(to_jsonb(history_alias))`);
fs.writeFileSync(path.join(here,'forward/08_candidate_context.sqlinc'),context);

let wf=own('../candidate-move-qr-paper-20261016/forward/05_native_paper_source.sqlinc');
wf=replace(wf,'  v_resubmit_source jsonb;','  v_resubmit_source jsonb;\n  v_family_context jsonb;\n  v_family_workflow public.candidate_submission_workflows%rowtype;\n  v_resubmit_pending jsonb;\n  v_creation_pending jsonb;');
wf=replace(wf,'  -- Genuine NEW submissions acquire the existing Candidate/family gate before',`  -- The exact Invoicef017235 locking-only kernel admits original immutable
  -- replay before fresh generation/state/target checks, including real pending.
  -- It grants no mutation or rebinding authority. Original native checks follow.
  if v_action not in('CREATE','RESUBMIT_REJECTED') then
    if p_session_id is not null then
      v_family_context:=private._candidate_session_context_v1(p_session_id,v_environment,null,p_now_utc,false);
      if nullif(v_family_context->>'selected_candidate_id','') is null or nullif(v_family_context->>'account_id','') is null then
        raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000';end if;
      perform private.timesheet_contract_expense_family_enter_v1(v_environment,p_workflow_id,
        nullif(v_family_context->>'selected_candidate_id','')::uuid,nullif(v_family_context->>'account_id','')::uuid);
    elsif v_is_service_action then
      -- Existing service-only function ACL and original service/Office
      -- admission below remain authority. This prefix grants only locks.
      perform private.timesheet_contract_expense_family_enter_v1(v_environment,p_workflow_id,null,null);
    elsif v_is_public_manager_action then
      if coalesce(v_payload->>'approval_token_hash_hex','') !~ '^[0-9a-fA-F]{64}$' then
        raise exception 'MANAGER_APPROVAL_REQUEST_NOT_READY' using errcode='28000';end if;
      select w.* into v_family_workflow from public.candidate_submission_workflows w
       join public.candidate_approval_requests a on a.workflow_id=w.id and a.workflow_generation=w.generation
        and a.review_manifest_sha256=w.review_manifest_sha256
       where w.id=p_workflow_id and w.environment=v_environment
        and a.token_hash=decode(v_payload->>'approval_token_hash_hex','hex') and a.state='PENDING'
        and a.method in('EMAIL','PHONE') and a.expires_at_utc>p_now_utc;
      if not found then raise exception 'MANAGER_APPROVAL_REQUEST_NOT_READY' using errcode='28000';end if;
      perform private.timesheet_contract_expense_family_enter_v1(v_environment,p_workflow_id,
        v_family_workflow.candidate_id,v_family_workflow.account_id);
    end if;
  end if;
  -- Genuine NEW submissions acquire the existing Candidate/family gate before`);
wf=replace(wf,"    if v_source_workflow.scope='WEEKLY' then",`    if v_source_workflow.scope='WEEKLY' then
      v_resubmit_pending:=private.timesheet_contract_native_pending_for_v1(coalesce(v_source_workflow.target_timesheet_id,v_source_workflow.anchor_timesheet_id));
      if v_resubmit_pending is null and v_source_workflow.target_timesheet_id is null and v_source_workflow.anchor_timesheet_id is null then
        v_resubmit_pending:=private.timesheet_contract_native_pending_week_v1(v_source_workflow.contract_week_id);end if;
      if v_resubmit_pending is not null then
        if not private.timesheet_contract_native_pending_workflow_v1(v_resubmit_pending,v_source_workflow.id)
          or v_source_workflow.workflow_kind='CONTRACT_EXPENSE' or v_source_workflow.rejection_scope='COMPLETE_EXPENSE_CLAIM' then
          raise exception 'CANDIDATE_NATIVE_REPLACEMENT_MEMBER_OWNER_REQUIRED' using errcode='55000';end if;
        v_resubmit_pending:=private.timesheet_contract_native_pending_enter_week_v1((v_resubmit_pending->>'contract_week_id')::uuid);
      end if;`);
wf=replace(wf,'where current_week.id=case when v_resubmit_move is null then v_source_workflow.contract_week_id',`where current_week.id=case when v_resubmit_pending is not null then (v_resubmit_pending->>'contract_week_id')::uuid
          when v_resubmit_move is null then v_source_workflow.contract_week_id`);
wf=replace(wf,'and current_week.contract_id is not distinct from case when v_resubmit_move is null then v_source_workflow.contract_id',`and current_week.contract_id is not distinct from case when v_resubmit_pending is not null then (v_resubmit_pending#>>'{placement,current_contract_id}')::uuid
          when v_resubmit_move is null then v_source_workflow.contract_id`);
const createValidated=`      perform private.candidate_workflow_factual_snapshot_assert_v1(v_payload->'input_snapshot');
    end if;
    if v_workflow_kind='DAILY' then`;
wf=replace(wf,createValidated,`      perform private.candidate_workflow_factual_snapshot_assert_v1(v_payload->'input_snapshot');
    end if;
    if v_scope='WEEKLY' and exists(select 1 from public.contract_weeks pending_week
      join public.contracts pending_contract on pending_contract.id=pending_week.contract_id and pending_contract.candidate_id=v_candidate_id
      where pending_week.id=nullif(v_payload->>'contract_week_id','')::uuid) then
      v_creation_pending:=private.timesheet_contract_native_pending_enter_week_v1(nullif(v_payload->>'contract_week_id','')::uuid);
    end if;
    if v_workflow_kind='DAILY' then`);
wf=replace(wf,"  if p_expected_generation is not null and v_workflow.generation<>p_expected_generation then",`  -- Original native immutable receipt above remains first. A fresh command
  -- cannot publish against the withdrawn historical target through ordinary
  -- null/unmatched fallback. New targetless current-CW work uses native CREATE.
  if v_action='WORKER_SUBMIT' and private.timesheet_contract_native_pending_for_v1(coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id)) is not null then
    raise exception 'CANDIDATE_NATIVE_PENDING_CURRENT_WEEK_REQUIRED' using errcode='55000';end if;
  if p_expected_generation is not null and v_workflow.generation<>p_expected_generation then`);
fs.writeFileSync(path.join(here,'forward/10_native_workflow_admission.sqlinc'),wf);
// Real pending/current Contract Week and immutable old history are distinct.
// The closed public Move17 fragment has no fabricated nullable UUID variant.
let detail=own('../candidate-contract-move-native-20261016/forward/05_detail_successor.sqlinc');
detail=replace(detail,'  v_evidence_origin_id uuid;','  v_evidence_origin_id uuid;\n  v_native_pending jsonb;\n  v_pending_history boolean:=false;');
detail=replace(detail,'  v_contract_move:=private.candidate_contract_move_context_v1',`  if p_workflow_id is not null then
    select w.* into v_workflow from public.candidate_submission_workflows w
     where w.id=p_workflow_id and w.candidate_id=v_candidate_id and w.environment=private._candidate_assert_environment(p_environment);
    if not found then raise exception 'CANDIDATE_DETAIL_NOT_FOUND' using errcode='P0002';end if;
    v_detail_source_timesheet_id:=case when v_workflow.workflow_kind='CONTRACT_EXPENSE' or v_workflow.rejection_scope='COMPLETE_EXPENSE_CLAIM'
      then coalesce(v_workflow.anchor_timesheet_id,v_workflow.target_timesheet_id)
      else coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id) end;
    v_native_pending:=private.timesheet_contract_native_pending_for_v1(v_detail_source_timesheet_id);
    if v_native_pending is null and v_detail_source_timesheet_id is null then
      v_native_pending:=private.timesheet_contract_native_pending_week_v1(v_workflow.contract_week_id);end if;
  elsif v_requested_timesheet_id is not null then
    v_native_pending:=private.timesheet_contract_native_pending_for_v1(v_requested_timesheet_id);
  elsif p_contract_week_id is not null then
    v_native_pending:=private.timesheet_contract_native_pending_week_v1(p_contract_week_id);
  end if;
  if v_native_pending is not null then
    if v_native_pending#>>'{placement,candidate_id}' is distinct from v_candidate_id::text
      or p_workflow_id is not null and not private.timesheet_contract_native_pending_workflow_v1(v_native_pending,p_workflow_id) then
      raise exception 'CANDIDATE_DETAIL_NOT_FOUND' using errcode='P0002';end if;
    v_pending_history:=coalesce(p_timesheet_id,v_detail_source_timesheet_id) is not null;
    if v_pending_history then
      p_timesheet_id:=coalesce(p_timesheet_id,v_detail_source_timesheet_id);
      -- Original A keeps its genuine CW. Withdrawn/rotated native B history
      -- uses the exact witnessed current B CW; no week/date-wide guess.
      select (select cw.id from public.contract_weeks cw join public.timesheets t on t.timesheet_id=p_timesheet_id
        where cw.timesheet_id=t.timesheet_id and cw.contract_id=t.contract_id and cw.week_ending_date=t.week_ending_date)
       into p_contract_week_id;
      if p_contract_week_id is null then
        if not exists(select 1 from public.timesheets t join public.contract_weeks cw on cw.id=(v_native_pending->>'contract_week_id')::uuid
          where t.timesheet_id=p_timesheet_id and t.contract_id=cw.contract_id and t.week_ending_date=cw.week_ending_date) then
          raise exception 'CANDIDATE_DETAIL_NOT_FOUND' using errcode='P0002';end if;
        p_contract_week_id:=(v_native_pending->>'contract_week_id')::uuid;
      end if;
    else
      p_timesheet_id:=null;p_contract_week_id:=(v_native_pending->>'contract_week_id')::uuid;
    end if;
  end if;
  v_contract_move:=private.candidate_contract_move_context_v1`);
detail=replace(detail,'  if p_workflow_id is not null and v_contract_move is null then','  if p_workflow_id is not null and v_contract_move is null and v_native_pending is null then');
detail=replace(detail,'where timesheet_id=p_timesheet_id and is_current=true and archived_at_utc is null;',`where timesheet_id=p_timesheet_id and ((is_current=true and archived_at_utc is null)
       or v_pending_history and v_native_pending is not null);`);
detail=replace(detail,"  if v_contract_move#>>'{dto,is_history}'='true' then","  if v_pending_history or v_contract_move#>>'{dto,is_history}'='true' then");
detail=replace(detail,"      or v_contract_move is not null and w.id in(select value::uuid from jsonb_array_elements_text(v_contract_move->'workflow_ids')))",`      or v_contract_move is not null and w.id in(select value::uuid from jsonb_array_elements_text(v_contract_move->'workflow_ids'))
      or v_native_pending is not null and private.timesheet_contract_native_pending_workflow_v1(v_native_pending,w.id))`);
detail=replace(detail,'  v_is_expense_only:=',`  if v_pending_history then v_tab_bucket:='HISTORY';end if;
  v_is_expense_only:=`);
fs.writeFileSync(path.join(here,'forward/11_candidate_detail.sqlinc'),detail);

let maps=own('../candidate-contract-move-native-20261016/forward/04_workflow_card_mapping.sqlinc');
maps=replace(maps,'  v_move jsonb;','  v_move jsonb;\n  v_pending jsonb;');
maps=replace(maps,'  if p_timesheet_id is not null and exists',`  v_pending:=case when p_timesheet_id is not null then private.timesheet_contract_native_pending_for_v1(p_timesheet_id)
    else private.timesheet_contract_native_pending_week_v1(p_contract_week_id) end;
  if v_pending is not null then
    return private.timesheet_contract_native_pending_workflow_v1(v_pending,p_workflow_id)
     and (p_contract_week_id is null or p_contract_week_id=(v_pending->>'contract_week_id')::uuid
       or p_contract_week_id=v_workflow.contract_week_id
       or exists(select 1 from public.contract_weeks cw where cw.id=p_contract_week_id and cw.timesheet_id=p_timesheet_id));
  end if;
  if p_timesheet_id is not null and exists`);
fs.writeFileSync(path.join(here,'forward/12_workflow_card_mapping.sqlinc'),maps);
let page=own('../candidate-contract-move-native-20261016/forward/07_page_v2_projection.sqlinc');
page=replace(page,'  v_move jsonb;','  v_move jsonb;\n  v_pending jsonb;');
page=replace(page,"    if v_move#>>'{dto,is_history}'='true' then",`    v_pending:=private.timesheet_contract_native_pending_for_v1(nullif(v_item->>'timesheet_id','')::uuid);
    if v_pending is not null and v_pending#>>'{placement,candidate_id}' is distinct from v_candidate_id::text then
      raise exception 'CANDIDATE_DETAIL_NOT_FOUND' using errcode='P0002';end if;
    if v_pending is not null or v_move#>>'{dto,is_history}'='true' then`);
fs.writeFileSync(path.join(here,'forward/13_page_v2.sqlinc'),page);
let summary=own('../candidate-contract-move-lifecycle-20261016/forward/06_summary_identity_invalidation.sqlinc');
summary=replace(summary,'  v_current_week_id uuid;','  v_current_week_id uuid;\n  v_native_pending jsonb;');
summary=replace(summary,'    select cw.id into strict v_current_week_id from public.contract_weeks cw',`    if v_current_timesheet_id is null then
      v_native_pending:=private.timesheet_contract_native_pending_for_v1(coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id));
      if v_native_pending is null then raise exception 'CURRENT_PLACEMENT_NATIVE_PENDING_UNBOUND' using errcode='55000';end if;
      v_current_week_id:=(v_native_pending->>'contract_week_id')::uuid;
      perform private._candidate_timesheet_summary_revision_upsert_v1('CONTRACT_WEEK',v_current_week_id,null,v_current_week_id);
    else
    select cw.id into strict v_current_week_id from public.contract_weeks cw`);
summary=replace(summary,"    perform private._candidate_timesheet_summary_revision_upsert_v1('CONTRACT_WEEK',v_current_week_id,v_current_timesheet_id,v_current_week_id);",`    perform private._candidate_timesheet_summary_revision_upsert_v1('CONTRACT_WEEK',v_current_week_id,v_current_timesheet_id,v_current_week_id);
    end if;`);
fs.writeFileSync(path.join(here,'forward/15_summary_pending.sqlinc'),summary);
console.log('Complete native pending/allocation/reader and authenticated workflow source generated; source review/qualification remains required.');
