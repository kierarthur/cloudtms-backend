import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';import crypto from 'node:crypto';
const here=path.dirname(fileURLToPath(import.meta.url));const root=process.argv[2];if(!root)throw new Error('ROOT_REQUIRED');
const read=p=>fs.readFileSync(p,'utf8').replace(/\r\n/g,'\n');
const replace=(s,a,b)=>{if(s.split(a).length!==2)throw new Error(`ANCHOR_NOT_EXACT:${a.slice(0,90)}`);return s.replace(a,b);};
const broker=read(path.join(root,'broker/src/candidate-app-backend.js'));
function fn(s,name){let a=s.indexOf(`async function ${name}(`);if(a<0)a=s.indexOf(`function ${name}(`);if(a<0)throw new Error(`FUNCTION_MISSING:${name}`);const b=s.indexOf('\n}\n',a);return s.slice(a,b+3).trimEnd()+'\n';}
let prepare=fn(broker,'prepareImmutableSubmission');
prepare=replace(prepare,'workflow, body, mutationKey)', 'workflow, body, mutationKey, access = null)');
prepare=replace(prepare,"  const authoritySubmission = workflow.workflow_kind === 'DAILY'",`  let calculationWorkflow = workflow;
  if (workflow.scope === 'WEEKLY' && access) {
    const detail = await rpcCall(deps, 'candidate_app_timesheet_detail_v1', candidateRpcArgs(access, env, {
      p_timesheet_id: null, p_contract_week_id: null, p_workflow_id: workflow.id
    }));
    if (detail?.contract_move != null) {
      const move = decodeCandidateContractMove(detail.contract_move);
      const week = detail.contract_week;
      if (!week || !UUID_RE.test(text(week.id)) || !UUID_RE.test(text(week.contract_id))
          || !isObject(detail.capabilities?.policy)
          || detail.timesheet?.id !== move.current_mutable_timesheet_id
          || !(Array.isArray(detail.workflows) ? detail.workflows : []).some(w =>
            w.workflow_id === workflow.id && Number(w.generation) === Number(workflow.generation))) {
        throw new CandidateHttpError(409, 'CANDIDATE_CONTEXT_STALE');
      }
      // Transient calculation input only. The stored workflow identity and the
      // original official-presentation/signed body below remain original A.
      calculationWorkflow = { ...workflow, contract_id: week.contract_id, contract_week_id: week.id,
        target_timesheet_id: move.current_mutable_timesheet_id, policy_snapshot_json: detail.capabilities.policy, _contract_move_current: true };
    }
  }
  const authoritySubmission = workflow.workflow_kind === 'DAILY'`);
prepare=replace(prepare,': await deps.buildWeeklySubmission({ workflow, factualSubmission: supplied, mutationKey });',': await deps.buildWeeklySubmission({ workflow: calculationWorkflow, factualSubmission: supplied, mutationKey });');
let builder=fn(read(path.join(root,'broker/src/index.js')),'buildCandidateWeeklySubmissionThroughCanonicalAuthority');
builder=replace(builder,"  const schedule = Array.isArray(factual.actual_schedule_json)",`  if (workflow._contract_move_current === true && (!existingTimesheet
      || String(existingTimesheet.timesheet_id) !== targetTimesheetId
      || String(existingTimesheet.contract_id) !== String(contract.id)
      || String(cw.timesheet_id) !== targetTimesheetId)) {
    throw new Error('CANDIDATE_CONTEXT_STALE');
  }
  const schedule = Array.isArray(factual.actual_schedule_json)`);
fs.writeFileSync(path.join(here,'forward/02_weekly_current_calculation.mjsinc'),prepare+'\n'+builder);
const callerOld='env, deps, workflow, { ...body, immutable_submission: normalisedSubmissionFacts }, mutationKey';
if(broker.split(callerOld).length!==2)throw new Error('SUBMIT_CALLER_NOT_EXACT');
fs.writeFileSync(path.join(here,'BROKER_CALLER.json'),JSON.stringify({path:'broker/src/candidate-app-backend.js',sourceSha256:crypto.createHash('sha256').update(fs.readFileSync(path.join(root,'broker/src/candidate-app-backend.js'))).digest('hex'),exactOld:callerOld,exactNew:callerOld+', access',functions:['prepareImmutableSubmission'],indexFunction:'buildCandidateWeeklySubmissionThroughCanonicalAuthority',publicProtocolChange:false},null,2)+'\n');
let sql=read(path.resolve(here,'../candidate-contract-move-native-20261016/forward/09_manager_start_successor.sqlinc'));
sql=replace(sql,'  v_contract_move jsonb;','  v_contract_move jsonb;\n  v_submit_move jsonb;\n  v_submit_workflow public.candidate_submission_workflows%rowtype;\n  v_submit_timesheet_id uuid;\n  v_submit_week_id uuid;\n  v_submit_contract_id uuid;\n  v_submit_break_context jsonb;\n  v_submit_candidate_id uuid;\n  v_submit_frozen_workflow jsonb;\n  v_submit_signature public.candidate_submission_components%rowtype;\n  v_submit_moved_at timestamptz;');
sql=replace(sql,'  if v_is_service_action then\n    if v_action=\'PAPER_PACK_RELEASE\' then',`  -- Genuine NEW submissions acquire the existing Candidate/family gate before
  -- workflow locks. Native immutable replay checks still return before fresh
  -- submission work below. Manager/replay/signed identities are not rebound.
  if v_action='WORKER_SUBMIT' and not exists(select 1 from public.audit_events ae
    where ae.object_type='candidate_workflow_mutation_receipt' and ae.object_id_text=p_workflow_id::text
     and ae.correlation_id=btrim(p_idempotency_key)) then
    select w.* into v_submit_workflow from public.candidate_submission_workflows w
      where w.id=p_workflow_id and w.environment=v_environment;
    if found and v_submit_workflow.scope='WEEKLY' then
      if not v_is_service_action then
        v_submit_candidate_id:=nullif(private._candidate_session_context_v1(p_session_id,v_environment,null,p_now_utc,false)->>'selected_candidate_id','')::uuid;
        if v_submit_candidate_id is null or v_submit_candidate_id<>v_submit_workflow.candidate_id then
          raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';end if;
      end if;
      v_submit_move:=private.candidate_contract_move_context_v1(v_environment,v_submit_workflow.candidate_id,null,v_submit_workflow.id);
      if v_submit_move is not null then
        perform private.timesheet_contract_move_family_lock_v1((v_submit_move#>>'{dto,original_timesheet_id}')::uuid);
        if private.candidate_contract_move_context_v1(v_environment,v_submit_workflow.candidate_id,null,v_submit_workflow.id) is distinct from v_submit_move then
          raise exception 'CANDIDATE_CONTEXT_STALE' using errcode='40001';end if;
        v_submit_timesheet_id:=(v_submit_move#>>'{dto,current_mutable_timesheet_id}')::uuid;
        v_submit_week_id:=(v_submit_move->>'current_week_id')::uuid;
        select t.contract_id into strict v_submit_contract_id from public.timesheets t where t.timesheet_id=v_submit_timesheet_id;
      end if;
    end if;
  end if;
  if v_is_service_action then
    if v_action='PAPER_PACK_RELEASE' then`);
sql=replace(sql,'select * into v_contract from public.contracts where id=v_workflow.contract_id;','select * into v_contract from public.contracts where id=coalesce(v_submit_contract_id,v_workflow.contract_id);');
sql=replace(sql,"      where id=v_workflow.contract_week_id\n        and contract_id=v_workflow.contract_id\n        and week_ending_date=v_workflow.week_ending_date\n      for update;", "      where id=coalesce(v_submit_week_id,v_workflow.contract_week_id)\n        and contract_id=coalesce(v_submit_contract_id,v_workflow.contract_id)\n        and week_ending_date=v_workflow.week_ending_date\n      for update;");
sql=replace(sql,'        where timesheet_id=v_workflow.target_timesheet_id\n          and timesheet_id=v_week.timesheet_id','        where timesheet_id=coalesce(v_submit_timesheet_id,v_workflow.target_timesheet_id)\n          and timesheet_id=v_week.timesheet_id');
sql=replace(sql,"      v_target_capabilities:=private._candidate_record_capabilities_v1(v_workflow.target_timesheet_id,v_week.id,'{}'::jsonb);\n      v_route_authority:=private._candidate_route_family_v1(v_workflow.target_timesheet_id,v_week.id);",`      v_target_capabilities:=private._candidate_record_capabilities_v1(coalesce(v_submit_timesheet_id,v_workflow.target_timesheet_id),v_week.id,'{}'::jsonb);
      v_route_authority:=private._candidate_route_family_v1(coalesce(v_submit_timesheet_id,v_workflow.target_timesheet_id),v_week.id);
      if v_submit_move is not null then
        -- Existing break-entry context token already binds actual physical UUID,
        -- Contract week and settings. Reuse it, with no new request protocol.
        v_submit_break_context:=private._candidate_break_entry_context_core_v1(v_submit_timesheet_id,v_submit_week_id,
          (p_now_utc at time zone 'Europe/London')::date,v_target_capabilities);
        if v_payload#>>'{break_entry_context,context_token}' is distinct from v_submit_break_context->>'context_token'
          or v_payload#>>'{break_entry_context,context_version}' is distinct from v_submit_break_context->>'context_version' then
          raise exception 'CANDIDATE_CONTEXT_STALE' using errcode='40001';end if;
        if coalesce(v_immutable_submission#>>'{canonical_tsfin_snapshot,rate_source_refs_json,contract_id}',
           v_immutable_submission#>>'{hours_submission,canonical_tsfin_snapshot,rate_source_refs_json,contract_id}')
            is distinct from v_submit_contract_id::text
          or coalesce(v_immutable_submission#>'{canonical_tsfin_snapshot,policy_snapshot_json}',
            v_immutable_submission#>'{hours_submission,canonical_tsfin_snapshot,policy_snapshot_json}') is distinct from v_policy then
          raise exception 'CANDIDATE_CURRENT_CALCULATION_UNBOUND' using errcode='40001';end if;
        select value into v_submit_frozen_workflow from jsonb_array_elements(v_submit_move#>'{source_member,from_evidence,workflows}') value
          where value->>'id'=v_workflow.id::text;
        if v_submit_frozen_workflow is null then
          select value into v_submit_frozen_workflow from jsonb_array_elements(v_submit_move#>'{original_evidence,workflows}') value
            where value->>'id'=v_workflow.id::text;end if;
        if v_submit_frozen_workflow is not null and (
          (case when private.candidate_contract_move_signature_facts_v1(coalesce(
             nullif(v_submit_frozen_workflow->'input_snapshot_json','null'::jsonb),v_submit_frozen_workflow->'immutable_submission_json'))
               =private.candidate_contract_move_signature_facts_v1('{}'::jsonb)
            then private.candidate_contract_move_signature_facts_v1(v_submit_move#>'{source_member,from_evidence,timesheet}')
            else private.candidate_contract_move_signature_facts_v1(coalesce(
              nullif(v_submit_frozen_workflow->'input_snapshot_json','null'::jsonb),v_submit_frozen_workflow->'immutable_submission_json')) end)
            is distinct from private.candidate_contract_move_signature_facts_v1(v_payload#>'{submission_request_identity,factual_submission}')
          or private.candidate_contract_move_facts_v1(to_jsonb(v_anchor_timesheet),'[]'::jsonb)
            is distinct from private.candidate_contract_move_facts_v1(v_submit_move#>'{source_member,from_evidence,timesheet}','[]'::jsonb)) then
          select o.completed_at_utc into strict v_submit_moved_at from private.timesheet_contract_move_operations o
            where o.id=(v_submit_move#>>'{dto,operation_id}')::uuid and o.state='COMPLETE';
          if private.candidate_contract_move_facts_v1(to_jsonb(v_anchor_timesheet),'[]'::jsonb)
            is distinct from private.candidate_contract_move_facts_v1(v_submit_move#>'{source_member,from_evidence,timesheet}','[]'::jsonb) then
            v_submit_moved_at:=greatest(v_submit_moved_at,v_anchor_timesheet.updated_at);end if;
          select c.* into v_submit_signature from public.candidate_submission_components c
            where c.id=nullif(v_payload->>'candidate_signature_component_id','')::uuid and c.workflow_id=v_workflow.id
              and c.component_kind='CANDIDATE_SIGNATURE' and c.state='IMMUTABLE' and c.source_content_sha256 is not null
              and c.source_component_id is null and c.created_at_utc>v_submit_moved_at and c.immutable_at_utc>v_submit_moved_at;
          if v_submit_signature.id is null then raise exception 'CANDIDATE_SIGNATURE_REQUIRED_AFTER_AMENDMENT' using errcode='55000';end if;
        end if;
      end if;`);
// Expense-only prospective placement uses separate actual component/claim owner;
// do not smuggle current-Contract calculation under its different signed policy.
sql=replace(sql,"    elsif v_workflow.workflow_kind='CONTRACT_EXPENSE' then\n      select week_row.* into v_anchor_week",`    elsif v_workflow.workflow_kind='CONTRACT_EXPENSE' then
      if v_submit_move is not null then raise exception 'CANDIDATE_MOVE_EXPENSE_NATIVE_SUBMISSION_NOT_QUALIFIED' using errcode='55000';end if;
      select week_row.* into v_anchor_week`);
sql=replace(sql,"jsonb_build_object('state',v_workflow.state,'generation',v_workflow.generation),\n    v_response,v_audit_reason,null,p_idempotency_key,p_now_utc);",`jsonb_build_object('state',v_workflow.state,'generation',v_workflow.generation)||case
      when v_action='WORKER_SUBMIT' and v_submit_move is not null then jsonb_build_object('contract_move_submission',jsonb_build_object(
        'version','CANDIDATE_MOVE_ACCEPTED_BODY_V1','workflow_id',v_workflow.id,'generation',v_workflow.generation,
        'operation_id',v_submit_move#>'{dto,operation_id}','placement_revision',v_submit_move#>'{dto,placement_revision}',
        'current_timesheet_id',v_submit_timesheet_id,
        'immutable_submission_sha256',encode(v_workflow.immutable_submission_sha256,'hex'),
        'input_snapshot_sha256',encode(private._candidate_sha256_jsonb_v1(v_workflow.input_snapshot_json),'hex'),
        'factual_submission_sha256',encode(private._candidate_sha256_jsonb_v1(
          private.candidate_contract_move_signature_facts_v1(v_workflow.input_snapshot_json)),'hex'),
        'semantic_request_sha256',v_mutation_request_sha256)) else '{}'::jsonb end,
    v_response,v_audit_reason,null,p_idempotency_key,p_now_utc);`);
fs.writeFileSync(path.join(here,'forward/03_worker_submit_current_placement.sqlinc'),sql.trimEnd()+'\n');
let context=read(path.resolve(here,'../candidate-contract-move-native-20261016/forward/03_candidate_projection.sqlinc'));
const oldFacts=context.slice(0,context.indexOf('create or replace function private.candidate_contract_move_context_v1('));
const newFacts=`-- Successor worked-fact domain. Identity/ancestry and accepted body proof are
-- independently checked by the native context and private workflow fact reader.
create or replace function private.candidate_contract_move_facts_v1(p_timesheet jsonb,p_workflows jsonb)
returns jsonb language sql immutable security definer set search_path=pg_catalog,private,pg_temp
as $function$
 select jsonb_build_object('version','CANDIDATE_MOVE_FACTS_V2',
  'timesheet',jsonb_build_object('week_ending_date',p_timesheet->'week_ending_date',
   'actual_schedule_json',p_timesheet->'actual_schedule_json','worked_start_iso',p_timesheet->'worked_start_iso',
   'worked_end_iso',p_timesheet->'worked_end_iso','worked_minutes',p_timesheet->'worked_minutes',
   'break_start_iso',p_timesheet->'break_start_iso','break_end_iso',p_timesheet->'break_end_iso','break_minutes',p_timesheet->'break_minutes',
   'additional_units_week',p_timesheet->'additional_units_week','additional_units_per_day',p_timesheet->'additional_units_per_day',
   'reference_number',p_timesheet->'reference_number','day_references_json',p_timesheet->'day_references_json'),
  'workflows',coalesce((select jsonb_agg(distinct facts order by facts) from (
    select coalesce(w->'worked_facts',private.candidate_contract_move_signature_facts_v1(
      coalesce(nullif(w->'input_snapshot_json','null'::jsonb),nullif(w->'immutable_submission_json','null'::jsonb),'{}'::jsonb))) facts
    from jsonb_array_elements(p_workflows) w
   ) projected where facts<>private.candidate_contract_move_signature_facts_v1('{}'::jsonb)
    and facts<>private.candidate_contract_move_signature_facts_v1(p_timesheet)),'[]'::jsonb));
$function$;

`;
fs.writeFileSync(path.join(here,'forward/02b_worked_facts.sqlinc'),newFacts.trimEnd()+`\nalter function private.candidate_contract_move_facts_v1(jsonb,jsonb) owner to postgres;\nrevoke all on function private.candidate_contract_move_facts_v1(jsonb,jsonb) from public,anon,authenticated,service_role;\n`);
context=context.replace(oldFacts,'');
context=replace(context,`select coalesce(jsonb_agg(jsonb_build_object('id',v.id,'immutable_submission_json',v.immutable_submission_json,
   'input_snapshot_json',v.input_snapshot_json) order by v.id),'[]'::jsonb)
  into native_workflows from public.candidate_submission_workflows v where v.id=any(workflow_ids);`,`select coalesce(jsonb_agg(jsonb_build_object('worked_facts',
   private.candidate_contract_move_workflow_facts_v1(v.id,coalesce((select frozen from jsonb_array_elements(source_member#>'{from_evidence,workflows}') frozen
      where frozen->>'id'=v.id::text),(select frozen from jsonb_array_elements(snapshot->'workflows') frozen where frozen->>'id'=v.id::text)))) order by v.id),'[]'::jsonb)
  into native_workflows from public.candidate_submission_workflows v where v.id=any(workflow_ids);`);
fs.writeFileSync(path.join(here,'forward/05_worked_fact_projection.sqlinc'),context.trimEnd()+'\n');
const coreFile='supabase/repeatable/28092026_1110_candidate_source_self_entry_break_context_v1.sql';
const coreSource=read(path.join(root,coreFile));
const coreStart=coreSource.indexOf('create or replace function private._candidate_break_entry_context_core_v1(');
const coreEnd=coreSource.indexOf('$function$;',coreSource.indexOf('as $function$',coreStart))+12;
let core=coreSource.slice(coreStart,coreEnd).trimEnd()+'\n';
core=replace(core,'  v_context_identity text;','  v_context_identity text;\n  v_placement jsonb;\n  v_current_financial jsonb;');
core=replace(core,"  return jsonb_build_object(\n    'applicable',v_applicable,",`  if exists(select 1 from private.timesheet_contract_family_aliases a where a.timesheet_id=v_timesheet.timesheet_id) then
    v_placement:=private.timesheet_contract_current_placement_v1(v_timesheet.timesheet_id);
    if v_placement->>'current_timesheet_id' is distinct from v_timesheet.timesheet_id::text then
      raise exception 'CANDIDATE_CONTEXT_STALE' using errcode='40001';end if;
    select to_jsonb(f) into v_current_financial from public.timesheets_financials f
      where f.timesheet_id=v_timesheet.timesheet_id and f.is_current=true;
    -- Existing opaque token gains current native row/revision binding. There is
    -- no new request/response field and no mutable workflow state in this seal.
    v_context_identity:=v_context_identity||'|'||encode(private.timesheet_contract_move_row_hash_v1(jsonb_build_object(
      'placement',v_placement,'timesheet',to_jsonb(v_timesheet),'contract_week',to_jsonb(v_week),
      'current_financial',v_current_financial,'policy',p_capabilities->'policy')),'hex');
  end if;
  return jsonb_build_object(
    'applicable',v_applicable,`);
core+=`\nalter function private._candidate_break_entry_context_core_v1(uuid,uuid,date,jsonb) owner to postgres;
revoke all on function private._candidate_break_entry_context_core_v1(uuid,uuid,date,jsonb) from public,anon,authenticated,service_role;\n`;
fs.writeFileSync(path.join(here,'forward/00a_break_token_row_binding.sqlinc'),core);

let touch=read(path.resolve(here,'../candidate-contract-move-native-20261016/forward/08_summary_touch_successor.sqlinc'));
touch=replace(touch,"v_move:=private.candidate_contract_move_context_v1(v_workflow.environment,v_workflow.candidate_id,v_current_timesheet_id,null);\n    v_current_timesheet_id:=(v_move#>>'{dto,current_timesheet_id}')::uuid;\n    v_current_week_id:=(v_move->>'current_week_id')::uuid;",`-- Cache identity invalidation runs inside workflow row triggers, before the
    -- same native command writes its immutable submit receipt. Strict H103 is
    -- sufficient here; outward readers separately validate the accepted body.
    v_move:=private.timesheet_contract_current_placement_v1(v_current_timesheet_id);
    v_current_timesheet_id:=(v_move->>'current_timesheet_id')::uuid;
    select cw.id into strict v_current_week_id from public.contract_weeks cw
      join public.timesheets t on t.timesheet_id=cw.timesheet_id and t.contract_id=cw.contract_id
      where cw.timesheet_id=v_current_timesheet_id and cw.week_ending_date=t.week_ending_date;`);
fs.writeFileSync(path.join(here,'forward/06_summary_identity_invalidation.sqlinc'),touch.trimEnd()+`\nalter function private._candidate_timesheet_summary_revision_touch_workflow_v1(uuid) owner to postgres;\nrevoke all on function private._candidate_timesheet_summary_revision_touch_workflow_v1(uuid) from public,anon,authenticated,service_role;\n`);
console.log('Distinct current-submission source generated; native NOT_RUN.');
