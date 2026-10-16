import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url));const root=process.argv[2];if(!root)throw Error('ROOT_REQUIRED');
const read=p=>fs.readFileSync(p,'utf8').replace(/\r\n/g,'\n');
const replace=(s,a,b)=>{if(s.split(a).length!==2)throw Error('ANCHOR_NOT_EXACT:'+a.slice(0,70));return s.replace(a,b);};
function fn(s,name){let a=s.indexOf(`async function ${name}(`);if(a<0)a=s.indexOf(`function ${name}(`);if(a<0)throw Error('FUNCTION_MISSING:'+name);const b=s.indexOf('\n}\n',a);return s.slice(a,b+3).trimEnd()+'\n';}
let sql=read(path.resolve(here,'../candidate-contract-move-lifecycle-20261016/forward/03_worker_submit_current_placement.sqlinc'));
sql=replace(sql,'  v_submit_moved_at timestamptz;','  v_submit_moved_at timestamptz;\n  v_paper_source_move jsonb;');
sql=replace(sql,"if v_submit_frozen_workflow is not null and (", "if v_is_electronic and not v_is_pending_expense_update and (v_submit_frozen_workflow is null or (");
sql=replace(sql,"v_submit_move#>'{source_member,from_evidence,timesheet}','[]'::jsonb)) then","v_submit_move#>'{source_member,from_evidence,timesheet}','[]'::jsonb))) then");
sql=replace(sql,"if v_submit_signature.id is null then raise exception 'CANDIDATE_SIGNATURE_REQUIRED_AFTER_AMENDMENT' using errcode='55000';end if;",`if v_submit_signature.id is null then raise exception 'CANDIDATE_SIGNATURE_REQUIRED_AFTER_AMENDMENT' using errcode='55000';end if;
          perform private.candidate_contract_move_draft_signature_assert_v1(v_workflow.id,
            v_payload#>'{submission_request_identity,factual_submission}',v_submit_signature.id);`);
sql=replace(sql,"jsonb_build_object('generation',v_workflow.generation,'preserved_source_component_count',v_component_no),",`jsonb_build_object('generation',v_workflow.generation,'preserved_source_component_count',v_component_no,
        'input_snapshot_sha256',encode(private._candidate_sha256_jsonb_v1(v_workflow.input_snapshot_json),'hex'),
        'semantic_request_sha256',v_mutation_request_sha256),`);
sql=replace(sql,"      perform 1 from public.timesheets\n      where timesheet_id=v_paper_timesheet_id and is_current=true and archived_at_utc is null\n      for update;",`      if exists(select 1 from private.timesheet_contract_family_aliases a where a.timesheet_id=v_paper_timesheet_id) then
        v_paper_source_move:=private.candidate_contract_move_context_v1(v_environment,v_workflow.candidate_id,v_paper_timesheet_id,v_workflow.id);
        if v_paper_source_move#>>'{dto,evidence_origin_timesheet_id}' is distinct from v_paper_timesheet_id::text then
          raise exception 'CANDIDATE_PAPER_TIMESHEET_NOT_READY' using errcode='55000';end if;
        perform private.timesheet_contract_move_family_lock_v1(v_paper_timesheet_id);
      end if;
      perform 1 from public.timesheets
      where timesheet_id=v_paper_timesheet_id and ((is_current=true and archived_at_utc is null)
        or (v_paper_source_move is not null and archived_reason_code='CONTRACT_MOVE' and not is_current and archived_at_utc is not null))
      for update;`);
sql=replace(sql,"    if not found then raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002'; end if;\n    if v_is_office_service_action",`    if not found then raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002'; end if;
    if v_paper_source_move is not null and private.candidate_contract_move_context_v1(v_environment,v_workflow.candidate_id,
      v_paper_timesheet_id,v_workflow.id) is distinct from v_paper_source_move then
      raise exception 'CANDIDATE_WORKFLOW_CONTEXT_CONFLICT' using errcode='40001';end if;
    if v_is_office_service_action`);
// Every genuine paper command retains the original workflow/manifest source.
// Acquire the accepted physical family before the existing Timesheet/workflow locks.
sql=replace(sql,"      v_paper_family_key:='CANDIDATE_PAPER_FAMILY:'",`      v_paper_source_move:=private.candidate_contract_move_paper_source_lock_v1(
        v_environment,v_candidate_id,v_workflow.id);
      v_paper_family_key:='CANDIDATE_PAPER_FAMILY:'`);
sql=replace(sql,"          and is_current=true\n          and archived_at_utc is null\n        for update;",`          and ((is_current=true and archived_at_utc is null) or
            (v_paper_source_move is not null and archived_reason_code='CONTRACT_MOVE'
             and not is_current and archived_at_utc is not null))
        for update;`);
sql=replace(sql,"      if v_action in ('PAPER_RETURN','AMEND','CANCEL','SUPERSEDE')\n         and v_workflow.updated_at_utc",`      if v_paper_source_move is not null and private.candidate_contract_move_context_v1(
        v_environment,v_candidate_id,v_paper_timesheet_id,v_workflow.id) is distinct from v_paper_source_move then
        raise exception 'CANDIDATE_WORKFLOW_CONTEXT_CONFLICT' using errcode='40001';end if;
      if v_action in ('PAPER_RETURN','AMEND','CANCEL','SUPERSEDE')
         and v_workflow.updated_at_utc`);
sql=replace(sql,"      v_route_authority:=private._candidate_route_family_v1(v_workflow.target_timesheet_id,v_workflow.contract_week_id);",`      v_route_authority:=private._candidate_route_family_v1(
        case when v_paper_source_move is not null then (v_paper_source_move#>>'{dto,current_timesheet_id}')::uuid
          else v_workflow.target_timesheet_id end,
        case when v_paper_source_move is not null then (v_paper_source_move->>'current_week_id')::uuid
          else v_workflow.contract_week_id end);`);
sql=replace(sql,"    if v_workflow.workflow_kind='CONTRACT_EXPENSE'\n       and v_workflow.target_timesheet_id is null then\n      execute",`    if (v_workflow.workflow_kind='CONTRACT_EXPENSE' and v_workflow.target_timesheet_id is null)
       or v_paper_source_move is not null then
      execute`);

// Preserve exact old CREATE replay before new-field validation/current checks.
// Native category resubmission has a distinct real internal snapshot owner.
sql=replace(sql,"    if v_workflow_kind='DAILY' then\n      if v_daily_first_source then",`    if v_scope='WEEKLY' and v_payload ? 'input_snapshot'
       and v_payload->'input_snapshot' is distinct from '{}'::jsonb
       and (v_workflow_kind in('CONTRACT_HOURS','CONTRACT_COMBINED')
         or (v_workflow_kind='CONTRACT_EXPENSE' and v_payload->'input_snapshot' ?| array[
           'actual_schedule_json','expense_claim','additional_units_week','additional_units_per_day','timesheet_patch_json','break_entry_context'])) then
      perform private.candidate_workflow_factual_snapshot_assert_v1(v_payload->'input_snapshot');
    end if;
    if v_workflow_kind='DAILY' then
      if v_daily_first_source then`);
fs.writeFileSync(path.join(here,'forward/01_native_draft_and_paper_release.sqlinc'),sql.trimEnd()+'\n');
const broker=read(path.join(root,'broker/src/candidate-app-backend.js'));
let context=fn(read(path.resolve(here,'../candidate-contract-move-native-20261016/forward/11_broker_native_contexts.mjsinc')),'candidatePaperPackContext');
context=replace(context,"  if (!qrRoute && !targetlessExpense) {",`  const inheritedOriginalPages=move?safePaperReturnPages(parseJson(workflows[0].paper_return_manifest_json,{})||{}):[];
  const inheritedOriginalPaper=move && inheritedOriginalPages.length>0 && inheritedOriginalPages.every(page=>page.manifest_version===2);
  if (!qrRoute && !targetlessExpense && !inheritedOriginalPaper) {`);
context=replace(context,'  if (!complete && version?.sha256) {',`  // An accepted original paper workflow's v2 pages already use the native
  // immutable workflow renderer. No generic document/archived TS write is needed.
  if (!complete && !version && move) {
    const originalWorkflow=workflows[0];
    const originalPages=safePaperReturnPages(parseJson(originalWorkflow.paper_return_manifest_json,{})||{});
    const bodySha=text(originalWorkflow.immutable_submission_sha256).replace(/^\\\\x/i,'').toLowerCase();
    if (!SHA256_RE.test(bodySha) || !originalPages.length || originalPages.some(page=>page.manifest_version!==2)
        || text(originalWorkflow.target_timesheet_id||originalWorkflow.anchor_timesheet_id)!==id) {
      throw new CandidateHttpError(409,'CANDIDATE_PAPER_PACK_IDENTITY_INVALID');
    }
    const sourceDetail=await rpcCall(deps,'candidate_app_timesheet_detail_v1',candidateRpcArgs(access,env,{
      p_timesheet_id:id,p_contract_week_id:null,p_workflow_id:originalWorkflow.id}));
    const sourceMove=decodeCandidateContractMove(sourceDetail?.contract_move);
    if (sourceMove.original_timesheet_id!==id || sourceMove.operation_id!==move.operation_id
       || sourceMove.placement_revision!==move.placement_revision
       || !(Array.isArray(sourceDetail.workflows)?sourceDetail.workflows:[]).some(w=>
          w.workflow_id===originalWorkflow.id&&Number(w.generation)===Number(originalWorkflow.generation))) {
      throw new CandidateHttpError(409,'CANDIDATE_CONTEXT_STALE');
    }
    version={id:null,r2_key:null,sha256:bodySha,status:'READY',source_kind:'WORKFLOW_IMMUTABLE_SUBMISSION'};
  }
  if (!complete && version?.sha256) {`);
let assemble=fn(broker,'assembleCandidatePaperPack');
assemble=replace(assemble,'  let base = null;\n  if (!targetlessExpense) {',`  const immutableOriginalSource=version?.source_kind==='WORKFLOW_IMMUTABLE_SUBMISSION'
    && SHA256_RE.test(text(version?.sha256)) && pages.every(page=>page.manifest_version===2);
  if (!targetlessExpense && version?.source_kind==='WORKFLOW_IMMUTABLE_SUBMISSION' && !immutableOriginalSource) {
    throw new CandidateHttpError(409,'CANDIDATE_PAPER_PACK_IDENTITY_INVALID');
  }
  let base = null;
  if (!targetlessExpense && !immutableOriginalSource) {`);
fs.writeFileSync(path.join(here,'forward/02_original_paper_pack.mjsinc'),context+'\n'+assemble);

const enqueueSource=read(path.join(root,'supabase/repeatable/06092026_0540_candidate_targetless_expense_paper_pack_v1.sql'));
const ea=enqueueSource.indexOf('create or replace function public.candidate_targetless_expense_paper_pack_enqueue_v1(');
const eb=enqueueSource.indexOf('$function$;',enqueueSource.indexOf('as $function$',ea))+'$function$;'.length;
let enqueue=enqueueSource.slice(ea,eb).trimEnd()+'\n';
enqueue=replace(enqueue,'  v_conflict_count integer;','  v_conflict_count integer;\n  v_move jsonb;\n  v_original_hours boolean:=false;\n  v_original_id uuid;');
enqueue=replace(enqueue,'  select workflow_row.* into v_workflow',`  select workflow_row.* into v_workflow from public.candidate_submission_workflows workflow_row
    where workflow_row.id=p_workflow_id and workflow_row.environment=v_environment;
  if found and v_workflow.workflow_kind in('CONTRACT_HOURS','CONTRACT_COMBINED') then
    v_original_id:=coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id);
    if exists(select 1 from private.timesheet_contract_family_aliases a where a.timesheet_id=v_original_id) then
      v_move:=private.candidate_contract_move_context_v1(v_environment,v_workflow.candidate_id,v_original_id,v_workflow.id);
      if v_move#>>'{dto,evidence_origin_timesheet_id}' is distinct from v_original_id::text then
        raise exception 'CANDIDATE_PAPER_PACK_IDENTITY_INVALID' using errcode='40001';end if;
      perform private.timesheet_contract_move_family_lock_v1(v_original_id);
      v_original_hours:=true;
    end if;
  end if;
  select workflow_row.* into v_workflow`);
enqueue=replace(enqueue,"  if v_workflow.workflow_kind<>'CONTRACT_EXPENSE'", "  if (not v_original_hours and v_workflow.workflow_kind<>'CONTRACT_EXPENSE')");
enqueue=replace(enqueue,"     or v_workflow.target_timesheet_id is not null\n     or v_workflow.anchor_timesheet_id is null", "     or (not v_original_hours and v_workflow.target_timesheet_id is not null)\n     or (not v_original_hours and v_workflow.anchor_timesheet_id is null)");
enqueue=replace(enqueue,"where timesheet_row.timesheet_id=v_workflow.anchor_timesheet_id\n    and timesheet_row.is_current=true\n    and timesheet_row.archived_at_utc is null", "where timesheet_row.timesheet_id=case when v_original_hours then v_original_id else v_workflow.anchor_timesheet_id end\n    and ((timesheet_row.is_current=true and timesheet_row.archived_at_utc is null) or\n      (v_original_hours and timesheet_row.archived_reason_code='CONTRACT_MOVE' and not timesheet_row.is_current and timesheet_row.archived_at_utc is not null))");
enqueue=replace(enqueue,"     or v_reserved_week.timesheet_id is not null then", "     or (not v_original_hours and v_reserved_week.timesheet_id is not null)\n     or (v_original_hours and v_reserved_week.timesheet_id is distinct from v_original_id) then");
enqueue=replace(enqueue,"  if not exists(\n    select 1 from public.contracts",`  if v_original_hours and (private.candidate_contract_move_context_v1(v_environment,v_workflow.candidate_id,v_original_id,v_workflow.id)
      is distinct from v_move or v_workflow.immutable_submission_sha256 is distinct from
        private._candidate_sha256_jsonb_v1(v_workflow.immutable_submission_json)
      or coalesce(v_workflow.paper_return_manifest_json->>'manifest_version','1') not in('1','2')) then
    raise exception 'CANDIDATE_PAPER_PACK_IDENTITY_INVALID' using errcode='40001';end if;
  if not exists(
    select 1 from public.contracts`);
enqueue=replace(enqueue,"'Printed expense claim – week ending '||v_workflow.week_ending_date::text,", "case when v_original_hours then 'Printed Timesheet – week ending ' else 'Printed expense claim – week ending ' end||v_workflow.week_ending_date::text,");
enqueue=replace(enqueue,"'<p>Please print every page of the attached expense claim, ask the manager to sign each page, and return a photograph of every signed page in MyTMS.</p>',","case when v_original_hours then '<p>Please print every page of the attached Timesheet, ask the manager to sign each page, and return a photograph of every signed page in MyTMS.</p>' else '<p>Please print every page of the attached expense claim, ask the manager to sign each page, and return a photograph of every signed page in MyTMS.</p>' end,");
enqueue=replace(enqueue,"'Please print every page of the attached expense claim, ask the manager to sign each page, and return a photograph of every signed page in MyTMS.',","case when v_original_hours then 'Please print every page of the attached Timesheet, ask the manager to sign each page, and return a photograph of every signed page in MyTMS.' else 'Please print every page of the attached expense claim, ask the manager to sign each page, and return a photograph of every signed page in MyTMS.' end,");
fs.writeFileSync(path.join(here,'forward/03_original_paper_mail_constructor.sqlinc'),enqueue);


let restart=fn(broker,'restartCandidatePaperSourceDocumentFromStatus');
restart=replace(restart,"  const documentState = upper(context?.timesheet?.document_state);",`  if (context?.version?.source_kind==='WORKFLOW_IMMUTABLE_SUBMISSION') {
    return {ok:true,skipped:true,reason:'WORKFLOW_SOURCE_READY'};
  }
  const documentState = upper(context?.timesheet?.document_state);`);
let status=fn(broker,'handlePaperPackStatus');
status=replace(status,"  if (context.state === 'PREPARING'\n      && upper(context.timesheet.document_state)","  if (context.version?.source_kind !== 'WORKFLOW_IMMUTABLE_SUBMISSION' && context.state === 'PREPARING'\n      && upper(context.timesheet.document_state)");
status=replace(status,"  if (context.state === 'PREPARING' && UUID_RE.test(documentOperationId) && deps.nudgeQrPack)","  if (context.version?.source_kind !== 'WORKFLOW_IMMUTABLE_SUBMISSION' && context.state === 'PREPARING' && UUID_RE.test(documentOperationId) && deps.nudgeQrPack)");
fs.writeFileSync(path.join(here,'forward/04_original_paper_status.mjsinc'),restart+'\n'+status);
function sqlfn(source,name){const a=source.indexOf('create or replace function '+name+'(');if(a<0)throw Error('SQL_FUNCTION_MISSING:'+name);const end=source.indexOf('$function$;',source.indexOf('as $function$',a));return source.slice(a,end+'$function$;'.length)+'\n';}
const promoteSource=read(path.join(root,'supabase/repeatable/30082026_1339_candidate_paper_manifest_page_qr_v2.sql'));
for(const [name,file] of [['public.candidate_paper_manifest_v2_promote_v1','05_original_paper_promote.sqlinc'],['public.candidate_paper_return_proof_validate_v2','06_original_paper_proof.sqlinc']]) {
 let body=sqlfn(promoteSource,name);
 body=replace(body,'  v_candidate_id uuid;','  v_candidate_id uuid;\n  v_source_move jsonb;');
 body=replace(body,'  select * into v_workflow\n  from public.candidate_submission_workflows',`  v_source_move:=private.candidate_contract_move_paper_source_lock_v1(
    private._candidate_assert_environment(p_environment),v_candidate_id,p_workflow_id);
  select * into v_workflow
  from public.candidate_submission_workflows`);
 body=replace(body,'    and is_current=true and archived_at_utc is null',`    and ((is_current=true and archived_at_utc is null) or
      (v_source_move is not null and archived_reason_code='CONTRACT_MOVE' and not is_current and archived_at_utc is not null))`);
 body=replace(body,"  if not found then raise exception 'CANDIDATE_TIMESHEET_NOT_FOUND' using errcode='P0002'; end if;",`  if not found then raise exception 'CANDIDATE_TIMESHEET_NOT_FOUND' using errcode='P0002'; end if;
  if v_source_move is not null and private.candidate_contract_move_context_v1(
    private._candidate_assert_environment(p_environment),v_candidate_id,v_timesheet.timesheet_id,v_workflow.id)
      is distinct from v_source_move then
    raise exception 'CANDIDATE_WORKFLOW_CONTEXT_CONFLICT' using errcode='40001';end if;`);
 fs.writeFileSync(path.join(here,'forward',file),body);
}

let proofV1=sqlfn(read(path.join(root,'supabase/repeatable/30082026_1531_candidate_paper_return_proof_transaction_boundary_reassert_v1.sql')),'public.candidate_paper_return_proof_validate_v1');
proofV1=replace(proofV1,'  v_candidate_id uuid;','  v_candidate_id uuid;\n  v_source_move jsonb;');
proofV1=replace(proofV1,'  select * into v_workflow from public.candidate_submission_workflows',`  v_source_move:=private.candidate_contract_move_paper_source_lock_v1(
    private._candidate_assert_environment(p_environment),v_candidate_id,p_workflow_id);
  select * into v_workflow from public.candidate_submission_workflows`);
proofV1=replace(proofV1,'    and is_current=true and archived_at_utc is null;',`    and ((is_current=true and archived_at_utc is null) or
      (v_source_move is not null and archived_reason_code='CONTRACT_MOVE' and not is_current and archived_at_utc is not null));`);
fs.writeFileSync(path.join(here,'forward/06a_original_paper_proof_v1.sqlinc'),proofV1);
let packComplete=sqlfn(read(path.join(root,'supabase/repeatable/30082026_1414_candidate_paper_return_pack_complete_v2.sql')),'public.candidate_paper_return_pack_complete_v2');
packComplete=replace(packComplete,'  select * into v_workflow\n  from public.candidate_submission_workflows',`  perform private.candidate_contract_move_paper_source_lock_v1(
    private._candidate_assert_environment(p_environment),v_candidate_id,p_workflow_id);
  select * into v_workflow
  from public.candidate_submission_workflows`);
fs.writeFileSync(path.join(here,'forward/07_original_paper_complete.sqlinc'),packComplete);


let createPayload=fn(broker,'normaliseCandidateWorkflowCreatePayload');
createPayload=replace(createPayload,'  const workflowKind = upper(supplied.workflow_kind);',`  const workflowKind = upper(supplied.workflow_kind);
  if(Object.hasOwn(supplied,'input_snapshot')) {
    if(upper(supplied.scope)!=='WEEKLY' || !['CONTRACT_HOURS','CONTRACT_COMBINED','CONTRACT_EXPENSE'].includes(workflowKind)) {
      throw new CandidateHttpError(400,'CANDIDATE_WORKFLOW_PAYLOAD_INVALID');
    }
    validateCandidateWorkflowFactualSnapshot(supplied.input_snapshot);
  }`);
fs.writeFileSync(path.join(here,'forward/09_create_payload.mjsinc'),createPayload);

console.log('Distinct native draft/paper continuation generated; native NOT_RUN.');
