// Reproduce complete native Candidate successors from pinned current owners.
// SQL installation/mount order belongs to the integrator, not this generator.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
const repo=fileURLToPath(new URL('../../../../',import.meta.url));
const directory=path.join(repo,'supabase/repeatable/healthroster_wp_u3_source_proposals');
const hash=s=>crypto.createHash('sha256').update(s).digest('hex');
const normalize=s=>s.replaceAll('\r\n','\n');
const pins={
  transition:['supabase/repeatable/10102026_1347_import_expense_qr_approval_v1.sql','a0a0b47eca6667fed5ea890abf71de2118adbcbeb2e39ad4b1a7007a51ce445c'],
  finalize:['supabase/repeatable/13102026_1331_candidate_finalize_next_import_expense_authority_v1.sql','5845f7ee77cb9164da6150bddbdbb90bd4e1c062fc556fa2e2207c78cc5e66fe']
};
export const earlyTransitionHook=`  -- Checklist lifecycle admission precedes all workflow/TS/TF business locks.
  -- Plain native session authentication does not replace the locked recheck.
  if v_action='WORKER_SUBMIT' and (p_session_id is not null or v_is_office_service_action) then
    if p_session_id is not null then
      v_context:=private._candidate_session_context_v1(p_session_id,v_environment,null,p_now_utc,false);
      v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
      if v_candidate_id is null then raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000'; end if;
    else
      select candidate_id into v_candidate_id from public.candidate_submission_workflows
        where id=p_workflow_id and environment=v_environment;
    end if;
    v_checklist_cycle_admission:=private.weekly_source_checklist_submission_cycle_admit_v1(
      p_workflow_id,v_environment,v_candidate_id);
  end if;

`;
export const transitionFrozenHook=`    -- The unchanged native owner has now stored the immutable HOURS claim,
    -- its real Candidate signature and artifact generation. No Weekly upload
    -- or publication is required to prepare genuine signed-client metadata.
    if v_checklist_cycle_admission->'applies'='true'::jsonb then
      if v_workflow.target_timesheet_id is distinct from (v_checklist_cycle_admission->>'timesheet_id')::uuid then
        raise exception 'WEEKLY_SOURCE_SUBMISSION_CYCLE_WORKFLOW_STALE' using errcode='40001';
      end if;
      perform private.weekly_source_checklist_submission_cycle_prepare_v1(v_workflow.target_timesheet_id,null);
    end if;

`;
export const earlyFinalizeHook=`  -- Resolve only the existing native workflow target. This acquires I1/F1
  -- before the unchanged finalizer's first workflow business lock; its later
  -- locked session/actor, generation, signature and receipt checks stay intact.
  select w.candidate_id into v_checklist_candidate_id from public.candidate_submission_workflows w
    where w.id=p_workflow_id and w.environment=v_environment;
  if p_session_id is not null then
    v_context:=private._candidate_session_context_v1(p_session_id,v_environment,null,p_now_utc,false);
    v_checklist_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
    if v_checklist_candidate_id is null then raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000'; end if;
  end if;
  perform private.weekly_source_checklist_submission_cycle_admit_v1(
    p_workflow_id,v_environment,v_checklist_candidate_id);

`;
export const finalizedHook=`  -- Retained native finalisation receipt now proves any artifact-generation
  -- bookkeeping advance. HOURS identity stays the real hours row, including
  -- a combined claim whose separate expense carrier is the final target.
  if v_hours_timesheet_id is not null then
    perform private.weekly_source_checklist_submission_cycle_prepare_v1(v_hours_timesheet_id,null);
  end if;
`;
function replaceOnce(source,before,after){
  if(source.split(before).length!==2)throw Error(`Native owner seam is not unique: ${before.slice(0,80)}`);
  return source.replace(before,after);
}
export function currentOwner(kind){
  const [relative,pin]=pins[kind],source=normalize(fs.readFileSync(path.join(repo,relative),'utf8'));
  if(hash(source)!==pin)throw Error(`Native predecessor changed: ${relative}`);
  const functionName=kind==='transition'?'candidate_workflow_transition_atomic_v1':'candidate_submission_finalize_atomic_v1';
  const start=source.indexOf(`create or replace function public.${functionName}(`);
  const grant=`grant execute on function public.${functionName}`;
  const end=source.indexOf(';',source.indexOf(grant,start))+1;
  if(start<0||end<1)throw Error('Complete native owner/ACL not found');
  return source.slice(start,end)+'\n';
}
export function forwardOwner(kind){
  let source=currentOwner(kind);
  if(kind==='transition'){
    source=replaceOnce(source,'  v_replaced_manager_signature_ids uuid[]:=array[]::uuid[];',
      '  v_replaced_manager_signature_ids uuid[]:=array[]::uuid[];\n  v_checklist_cycle_admission jsonb;');
    source=replaceOnce(source,'  if v_is_service_action then\n',earlyTransitionHook+'  if v_is_service_action then\n');
    source=replaceOnce(source,"  elsif v_action='REGISTER_REVIEW_COMPONENT' then\n",transitionFrozenHook+"  elsif v_action='REGISTER_REVIEW_COMPONENT' then\n");
  }else{
    source=replaceOnce(source,'  v_completion_after jsonb;','  v_completion_after jsonb;\n  v_checklist_candidate_id uuid;');
    source=replaceOnce(source,'  select * into v_workflow from public.candidate_submission_workflows where id=p_workflow_id for update;',
      earlyFinalizeHook+'  select * into v_workflow from public.candidate_submission_workflows where id=p_workflow_id for update;');
    source=replaceOnce(source,'  return v_response;\nexception\n',finalizedHook+'  return v_response;\nexception\n');
  }
  return source;
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
  for(const kind of ['transition','finalize']){
    for(const direction of ['forward','reverse']){
      const body=direction==='forward'?forwardOwner(kind):currentOwner(kind);
      fs.writeFileSync(path.join(directory,`wp_u3_candidate_${kind}.${direction}.sqlinc`),
        `-- SOURCE ONLY: complete ${direction} native ${kind} owner. No repeatable root.\n`+body);
    }
  }
  console.log('Four complete pinned Candidate owner/ACL proposal leaves generated. No SQL executed.');
}
