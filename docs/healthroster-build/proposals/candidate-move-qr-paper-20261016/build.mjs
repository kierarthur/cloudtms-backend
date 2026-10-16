import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),root=process.argv[2];if(!root)throw Error('ROOT_REQUIRED');
const read=p=>fs.readFileSync(p,'utf8').replace(/\r\n/g,'\n'),own=p=>read(path.resolve(here,p));
const replace=(s,a,b)=>{if(s.split(a).length!==2)throw Error('ANCHOR_NOT_EXACT:'+a.slice(0,90));return s.replace(a,()=>b);};
let binding=own('../candidate-move-native-reset-20261016/forward/00_native_rotation_binding.sqlinc');
binding=binding.replaceAll("ae.action='CANDIDATE_ELECTRONIC_REJECTED_VERSION_ROTATED'","ae.action in('CANDIDATE_ELECTRONIC_REJECTED_VERSION_ROTATED','QR_HOURS_REFUSED')");
fs.writeFileSync(path.join(here,'forward/00_native_rotation_bindings.sqlinc'),binding);
let publish=own('../candidate-move-native-reset-20261016/forward/01_native_rotation_publish.sqlinc');
publish=publish.replaceAll('private.timesheet_contract_native_rotation_publish_v1','private.timesheet_contract_native_qr_rotation_publish_v1').replaceAll('CANDIDATE_ELECTRONIC_REJECTED_VERSION_ROTATED','QR_HOURS_REFUSED');
publish=replace(publish,"jsonb_build_object('old_timesheet_id',old_t.timesheet_id,'old_version',old_t.version,","jsonb_build_object('booking_id',old_t.booking_id,'old_timesheet_id',old_t.timesheet_id,'old_version',old_t.version,");
publish=replace(publish,"'submission_mode','MANUAL','effective_submission_mode','ELECTRONIC',","'submission_mode','MANUAL','effective_submission_mode','QR',");
fs.writeFileSync(path.join(here,'forward/01_native_qr_publish.sqlinc'),publish);
const source=read(path.join(root,'supabase/repeatable/07082026_2225_candidate_app_qr_settings_invoice_replacements_v1.sql'));
const qa=source.indexOf('create or replace function public.timesheet_qr_refuse_and_reset('),qb=source.indexOf('\n$$;',qa);
if(qa<0||qb<0)throw Error('QR_NATIVE_FUNCTION_NOT_FOUND');let qr=(source.slice(qa,qb+4)+'\n').replace('as $$','as $function$').replace('\n$$;','\n$function$;');
qr=replace(qr,'set search_path = public','set search_path = pg_catalog, public, private, extensions, pg_temp');
qr=replace(qr,'  v_weekly_source_guard jsonb;','  v_weekly_source_guard jsonb;\n  v_move jsonb;\n  v_old_placement jsonb;');
qr=replace(qr,'  -- 0) Resolve booking_id from ANY row (stale or current)',`  if exists(select 1 from private.timesheet_contract_family_aliases a where a.timesheet_id=p_timesheet_id) then
    v_move:=private.timesheet_contract_move_family_lock_v1(p_timesheet_id);
    if v_move->>'current_timesheet_id' is distinct from p_expected_timesheet_id::text then
      raise exception 'TIMESHEET_MOVED' using errcode='40001';end if;
    select to_jsonb(p) into strict v_old_placement from private.timesheet_contract_current_placements p
      join private.timesheet_contract_family_aliases a on a.family_root_id=p.family_root_id and a.member_key=p.member_key
      where a.timesheet_id=p_timesheet_id;
    if v_old_placement->>'role' is distinct from 'WORK' or exists(select 1 from private.timesheet_contract_current_placements sibling
      where sibling.family_root_id=(v_old_placement->>'family_root_id')::uuid and sibling.role='CORRECTION_MEMBER') then
      raise exception 'CANDIDATE_NATIVE_CORRECTION_PAIR_REQUIRED' using errcode='55000';end if;
  end if;
  -- 0) Resolve booking_id from ANY row (stale or current)`);
const ga=qr.indexOf('  v_weekly_source_guard := '),gb=qr.indexOf('  v_new_version :=',ga);if(ga<0||gb<ga)throw Error('QR_GUARD_NOT_FOUND');
const guard=qr.slice(ga,gb),og=replace(guard,'    v_current.timesheet_id',`    (select a.original_timesheet_id from private.timesheet_contract_family_aliases a where a.timesheet_id=v_current.timesheet_id)`);
qr=qr.slice(0,ga)+'  if v_old_placement is not null then\n'+og+'  end if;\n'+qr.slice(ga);
qr=replace(qr,'  -- 6) Reset TSFIN in-place and MOVE it to new timesheet_id',`  if v_old_placement is not null then
    perform private.timesheet_contract_native_qr_rotation_publish_v1(v_old_placement,v_new_timesheet_id,p_reason,p_actor_user_id,v_now);
  end if;
  -- 6) Reset TSFIN in-place and MOVE it to new timesheet_id`);
qr=replace(qr,'  -- 8) Audit event\n  insert into public.audit_events(', '  -- The genuine moved QR event was emitted before native financial callbacks.\n  if v_old_placement is null then\n  -- 8) Audit event\n  insert into public.audit_events(');
qr=replace(qr,'  -- Return\n  timesheet_id :=','  end if;\n  -- Return\n  timesheet_id :=');
fs.writeFileSync(path.join(here,'forward/02_native_qr_reset.sqlinc'),qr);
// Existing complete full workflow successor: use actual captured per-workflow
// source, never assume immutable family evidence A is every workflow's page.
let wf=own('../candidate-move-native-reset-20261016/forward/08_native_resubmit.sqlinc');
wf=replace(wf,`        v_paper_source_move:=private.candidate_contract_move_context_v1(v_environment,v_workflow.candidate_id,v_paper_timesheet_id,v_workflow.id);
        if v_paper_source_move#>>'{dto,evidence_origin_timesheet_id}' is distinct from v_paper_timesheet_id::text then
          raise exception 'CANDIDATE_PAPER_TIMESHEET_NOT_READY' using errcode='55000';end if;
        perform private.timesheet_contract_move_family_lock_v1(v_paper_timesheet_id);`,`        v_paper_source_move:=private.candidate_contract_move_paper_source_lock_v1(v_environment,v_workflow.candidate_id,v_workflow.id);`);
fs.writeFileSync(path.join(here,'forward/05_native_paper_source.sqlinc'),wf);
let enqueue=own('../candidate-move-paper-draft-20261016/forward/03_original_paper_mail_constructor.sqlinc');
enqueue=replace(enqueue,`      v_move:=private.candidate_contract_move_context_v1(v_environment,v_workflow.candidate_id,v_original_id,v_workflow.id);
      if v_move#>>'{dto,evidence_origin_timesheet_id}' is distinct from v_original_id::text then
        raise exception 'CANDIDATE_PAPER_PACK_IDENTITY_INVALID' using errcode='40001';end if;
      perform private.timesheet_contract_move_family_lock_v1(v_original_id);
      v_original_hours:=true;`,`      v_move:=private.candidate_contract_move_paper_source_lock_v1(v_environment,v_workflow.candidate_id,v_workflow.id);
      v_original_hours:=v_move is not null;`);
fs.writeFileSync(path.join(here,'forward/04_captured_paper_constructor.sqlinc'),enqueue);
let worker=own('../candidate-move-paper-draft-20261016/forward/02_original_paper_pack.mjsinc');
worker=replace(worker,'  if (move) {\n',`  let capturedWorkflows = null;
  if (move) {
`);
worker=replace(worker,'    id = move.evidence_origin_timesheet_id;',`    // Only native closed detail membership may select a workflow captured on
    // intermediate B. A family label or original-A assumption is insufficient.
    const paperMembers=(Array.isArray(detail.workflows)?detail.workflows:[]).filter(w=>
      upper(w.route)==='PAPER'&&upper(w.state)==='AWAITING_PAPER_RETURN');
    if (paperMembers.length!==1) throw new CandidateHttpError(409,
      paperMembers.length?'CANDIDATE_PAPER_WORKFLOW_CONFLICT':'CANDIDATE_PAPER_WORKFLOW_NOT_READY');
    const nativeWorkflow=await workflowRow(env,requireUuid(paperMembers[0].workflow_id,'CANDIDATE_WORKFLOW_NOT_FOUND'));
    id=requireUuid(nativeWorkflow.target_timesheet_id||nativeWorkflow.anchor_timesheet_id,'CANDIDATE_PAPER_PACK_IDENTITY_INVALID');
    if (nativeWorkflow.environment!==environmentName(env)
       || nativeWorkflow.candidate_id!==access.selected_candidate_id
       || nativeWorkflow.account_id!==access.account_id
       || nativeWorkflow.id!==paperMembers[0].workflow_id
       || Number(nativeWorkflow.generation)!==Number(paperMembers[0].generation)
       || upper(nativeWorkflow.route)!=='PAPER'||upper(nativeWorkflow.state)!=='AWAITING_PAPER_RETURN') {
      throw new CandidateHttpError(409,'CANDIDATE_CONTEXT_STALE');
    }
    const capturedDetail=await rpcCall(deps,'candidate_app_timesheet_detail_v1',candidateRpcArgs(access,env,{
      p_timesheet_id:id,p_contract_week_id:null,p_workflow_id:nativeWorkflow.id}));
    const capturedMove=decodeCandidateContractMove(capturedDetail?.contract_move);
    if (capturedMove.operation_id!==move.operation_id||capturedMove.family_root_id!==move.family_root_id
      || capturedMove.placement_revision!==move.placement_revision
      || !(Array.isArray(capturedDetail.workflows)?capturedDetail.workflows:[]).some(w=>
        w.workflow_id===nativeWorkflow.id&&Number(w.generation)===Number(nativeWorkflow.generation)
        && text(w.target_timesheet_id||w.anchor_timesheet_id)===id)) {
      throw new CandidateHttpError(409,'CANDIDATE_CONTEXT_STALE');
    }
    capturedWorkflows=[nativeWorkflow];`);
worker=replace(worker,'  const workflows = await activePaperWorkflowsForTimesheet(env, id);','  const workflows = capturedWorkflows || await activePaperWorkflowsForTimesheet(env, id);');
worker=replace(worker,'    if (sourceMove.original_timesheet_id!==id || sourceMove.operation_id!==move.operation_id','    if (sourceMove.family_root_id!==move.family_root_id || sourceMove.operation_id!==move.operation_id');
fs.writeFileSync(path.join(here,'forward/06_captured_paper_context.mjsinc'),worker);

function sqlfn(s,n){const a=s.indexOf('create or replace function '+n+'('),b=s.indexOf('$function$;',s.indexOf('as $function$',a));if(a<0||b<0)throw Error('FUNCTION_MISSING:'+n);return s.slice(a,b+11)+'\n';}
const retireSource=read(path.join(root,'supabase/repeatable/07082026_2120_candidate_workflow_transition_atomic_v1.sql'));
let single=sqlfn(retireSource,'private._candidate_paper_delivery_retire_v1');
single=replace(single,'  v_current_generation_token_owned boolean:=false;','  v_current_generation_token_owned boolean:=false;\n  v_move jsonb;\n  v_generation_admission_revoked boolean:=false;\n  v_captured_primary_count integer:=0;');
single=replace(single,'  select workflow.* into v_workflow',`  select workflow.* into v_workflow from public.candidate_submission_workflows workflow where workflow.id=p_workflow_id;
  if found then
    v_move:=private.candidate_contract_move_paper_source_lock_v1(v_workflow.environment,v_workflow.candidate_id,v_workflow.id);
  end if;
  select workflow.* into v_workflow`);
single=replace(single,"  if nullif(btrim(coalesce(v_qr_source.booking_id,'')),'') is not null then",`  if v_move is not null then
    if v_qr_source.timesheet_id is distinct from v_rejected_target_timesheet_id then
      raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT' using errcode='40001';end if;
    v_qr_current:=v_qr_source;
  elsif nullif(btrim(coalesce(v_qr_source.booking_id,'')),'') is not null then`);
single=replace(single,"  if nullif(btrim(coalesce(v_qr_current.qr_token,'')),'') is null then",`  if v_move is not null then
    -- Exact generation delivery was retired above. The native v1/v2 proof
    -- owners below require that real unretired receipt for captured sources.
    -- Preserve archived legacy QR/signature bytes and report no token mutation.
    v_generation_admission_revoked:=not exists(select 1 from public.mail_outbox m
      where m.type='TIMESHEET_QR' and m.context_kind='timesheets'
       and m.context_id=v_qr_source.timesheet_id
       and m.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
       and m.payment_scope_json->>'candidate_workflow_generation'=v_delivery_generation::text
       and m.payment_scope_json->'candidate_paper_generation_retired' is distinct from 'true'::jsonb);
    if not v_generation_admission_revoked or v_mail_retired_count<>v_mail_count then
      raise exception 'CANDIDATE_PAPER_QR_INVALIDATION_NOT_PROVEN' using errcode='40001';end if;
  elsif nullif(btrim(coalesce(v_qr_current.qr_token,'')),'') is null then`);
single=single.replaceAll("'qr_already_invalidated',v_qr_already_invalidated", "'qr_already_invalidated',v_qr_already_invalidated,'generation_admission_revoked',v_generation_admission_revoked");
single=replace(single,'  select workflow.* into v_workflow\n  from public.candidate_submission_workflows workflow',`  if v_move is not null then
    perform pg_advisory_xact_lock(hashtextextended('CANDIDATE_PAPER_FAMILY:'||v_workflow.environment||':'
      ||coalesce(v_workflow.contract_id::text,'-')||':'||coalesce(v_workflow.week_ending_date::text,v_workflow.work_date::text,'-'),0));
  end if;
  select workflow.* into v_workflow
  from public.candidate_submission_workflows workflow`);
single=replace(single,"    if v_mail.status='SENT' and not v_is_historical_replacement_retirement then",`    if v_move is not null then
      if v_mail.context_id is distinct from v_rejected_target_timesheet_id
        or coalesce(v_mail.payment_scope_json->>'candidate_mail_authority','') not in('CANDIDATE_PAPER_V1','CANDIDATE_PAPER_PACK_EMAIL_V1')
        or lower(coalesce(v_mail.payment_scope_json->>'paper_return_manifest_sha256','')) !~ '^[0-9a-f]{64}$'
        or (not v_is_historical_replacement_retirement and lower(v_mail.payment_scope_json->>'paper_return_manifest_sha256')
          is distinct from encode(v_workflow.paper_return_manifest_sha256,'hex')) then
        raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT' using errcode='40001';end if;
      if v_mail.payment_scope_json->>'candidate_mail_authority'='CANDIDATE_PAPER_V1' then
        if coalesce(v_mail.payment_scope_json->>'qr_token_hash','') !~ '^[0-9a-fA-F]{64}$'
          and (v_mail.payment_scope_json->>'source_authority' is distinct from 'WORKFLOW_IMMUTABLE_SUBMISSION'
            or (not v_is_historical_replacement_retirement and v_mail.payment_scope_json->>'source_authority_sha256'
              is distinct from encode(v_workflow.immutable_submission_sha256,'hex'))) then
          raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT' using errcode='40001';end if;
        v_captured_primary_count:=v_captured_primary_count+1;
      end if;
    end if;
    if v_mail.status='SENT' and not v_is_historical_replacement_retirement then`);
single=replace(single,'  if v_mail_count<1 or v_qr_source_timesheet_id is null then','  if v_mail_count<1 or v_qr_source_timesheet_id is null or (v_move is not null and v_captured_primary_count<1) then');
fs.writeFileSync(path.join(here,'forward/07_captured_paper_retire.sqlinc'),single);
let set=sqlfn(retireSource,'private._candidate_paper_delivery_retire_set_v1');
set=replace(set,'  v_source_already_invalidated boolean;',`  v_source_already_invalidated boolean;
  v_move jsonb;
  v_captured_ids uuid[]:='{}'::uuid[];
  v_captured_generations integer[]:='{}'::integer[];
  v_ordinary_ids uuid[]:='{}'::uuid[];
  v_ordinary_generations integer[]:='{}'::integer[];`);
const gate=`  -- Establish the common family lock order before freezing any selected`;
set=replace(set,gate,`  -- Certified physical family gates precede every existing PAPER/WF lock.
  -- Partition only real captured sources. The ordinary complete owner below
  -- remains the authority for current QR families, including mixed selections.
  for v_input in select distinct accepted.candidate_id,a.family_root_id,a.timesheet_id
    from unnest(p_workflow_ids) input(workflow_id)
    join public.candidate_submission_workflows w on w.id=input.workflow_id
    join private.timesheet_contract_family_aliases a on a.timesheet_id=coalesce(w.target_timesheet_id,w.anchor_timesheet_id)
    join private.timesheet_contract_move_operations accepted on accepted.id=a.accepted_operation_id
    order by accepted.candidate_id,a.family_root_id,a.timesheet_id
  loop perform private.timesheet_contract_move_family_lock_v1(v_input.timesheet_id);end loop;
  for v_input in select i.workflow_id,i.expected_generation from unnest(p_workflow_ids,p_expected_generations) i(workflow_id,expected_generation)
    order by i.workflow_id
  loop
    select w.* into v_workflow from public.candidate_submission_workflows w where w.id=v_input.workflow_id;
    if not found then raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';end if;
    v_move:=private.candidate_contract_move_paper_source_lock_v1(v_workflow.environment,v_workflow.candidate_id,v_workflow.id);
    if v_move is null then
      v_ordinary_ids:=array_append(v_ordinary_ids,v_workflow.id);v_ordinary_generations:=array_append(v_ordinary_generations,v_input.expected_generation);
    else
      v_captured_ids:=array_append(v_captured_ids,v_workflow.id);v_captured_generations:=array_append(v_captured_generations,v_input.expected_generation);
    end if;
  end loop;
  if cardinality(v_captured_ids)>0 then
    for v_family_key in select distinct 'CANDIDATE_PAPER_FAMILY:'||w.environment||':'||coalesce(w.contract_id::text,'-')||':'
      ||coalesce(w.week_ending_date::text,w.work_date::text,'-') from public.candidate_submission_workflows w
      where w.id=any(p_workflow_ids) order by 1
    loop perform pg_advisory_xact_lock(hashtextextended(v_family_key,0));end loop;
    perform 1 from public.candidate_submission_workflows w where w.id=any(p_workflow_ids) order by w.id for update;
    if cardinality(v_ordinary_ids)>0 then
      -- One bounded recursive call contains no captured source and executes the
      -- unchanged native source-wide token retirement and shared-owner checks.
      v_result:=private._candidate_paper_delivery_retire_set_v1(v_ordinary_ids,v_ordinary_generations,p_reason_code,p_now_utc);
      if v_result->'retired' is distinct from 'true'::jsonb or v_result->'qr_invalidation_proven' is distinct from 'true'::jsonb then
        raise exception 'CANDIDATE_PAPER_QR_INVALIDATION_NOT_PROVEN' using errcode='40001';end if;
      v_retirement_receipts:=v_result->'retirement_receipts';v_source_receipts:=v_result->'source_receipts';
    end if;
    for v_input in select i.workflow_id,i.expected_generation from unnest(v_captured_ids,v_captured_generations) i(workflow_id,expected_generation)
      order by i.workflow_id
    loop
      v_result:=private._candidate_paper_delivery_retire_v1(v_input.workflow_id,v_input.expected_generation,p_reason_code,p_now_utc);
      if v_result->'retired' is distinct from 'true'::jsonb or v_result->'generation_admission_revoked' is distinct from 'true'::jsonb then
        raise exception 'CANDIDATE_PAPER_QR_INVALIDATION_NOT_PROVEN' using errcode='40001';end if;
      v_retirement_receipts:=v_retirement_receipts||jsonb_build_array(v_result);
      v_source_receipts:=v_source_receipts||jsonb_build_array(jsonb_build_object('qr_source_timesheet_id',v_result->'qr_source_timesheet_id',
        'qr_invalidated',false,'qr_already_invalidated',false,'generation_admission_revoked',true));
    end loop;
    return jsonb_build_object('retired',true,'qr_invalidation_proven',true,
      'selected_workflow_count',cardinality(p_workflow_ids),'selected_workflow_ids',to_jsonb(p_workflow_ids),
      'source_count',jsonb_array_length(v_source_receipts),'source_receipts',v_source_receipts,
      'retirement_receipts',v_retirement_receipts,'preserved_workflows',v_preserved_workflows,'reason_code',v_reason);
  end if;
  -- Establish the common family lock order before freezing any selected`);
fs.writeFileSync(path.join(here,'forward/08_captured_paper_retire_set.sqlinc'),set);
// The genuine native page proof requires an unretired real outbox on a captured
// source. This is the actual admission cutoff used by retirement above.
for(const [input,output] of [['06_original_paper_proof.sqlinc','09_captured_paper_proof_v2.sqlinc'],['06a_original_paper_proof_v1.sqlinc','10_captured_paper_proof_v1.sqlinc']]){
 let proof=own('../candidate-move-paper-draft-20261016/forward/'+input);
 proof=replace(proof,"  v_manifest_hex:=encode(v_workflow.paper_return_manifest_sha256,'hex');",`  if v_source_move is not null and not exists(select 1 from public.mail_outbox m
    where m.type='TIMESHEET_QR' and m.context_kind='timesheets'
     and m.context_id=coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id)
     and m.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
     and m.payment_scope_json->>'candidate_workflow_generation'=v_workflow.generation::text
     and m.payment_scope_json->>'candidate_mail_authority'='CANDIDATE_PAPER_V1'
     and lower(m.payment_scope_json->>'paper_return_manifest_sha256')=encode(v_workflow.paper_return_manifest_sha256,'hex')
     and (m.payment_scope_json->'candidate_paper_generation_retired'='false'::jsonb
       or (not(m.payment_scope_json ? 'candidate_paper_generation_retired') and not exists(select 1 from public.audit_events retired
         where retired.object_type='candidate_submission_workflow' and retired.object_id_text=v_workflow.id::text
          and retired.action='CANDIDATE_PAPER_DELIVERY_RETIRED'
          and retired.after_json->>'delivery_generation'=v_workflow.generation::text)))) then
    raise exception 'CANDIDATE_PAPER_QR_PROOF_STALE' using errcode='40001';end if;
  v_manifest_hex:=encode(v_workflow.paper_return_manifest_sha256,'hex');`);
 fs.writeFileSync(path.join(here,'forward/'+output),proof);
}
console.log('Complete QR allocator, captured source and actual native retirement/page cutoff successors generated; native NOT_RUN.');
