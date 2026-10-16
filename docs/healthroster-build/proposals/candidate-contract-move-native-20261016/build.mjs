import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url));
const root=process.argv[2];
if(!root)throw new Error('SOURCE_ROOT_REQUIRED');
const base=path.resolve(here,'../checklist-v6-contract-correction-20261016/forward/sql');
const read=(p)=>fs.readFileSync(p,'utf8').replace(/\r\n/g,'\n');
const hash=(s)=>crypto.createHash('sha256').update(s).digest('hex');
const own=(name)=>read(path.join(base,name));
function body(source,name){const marker=`create or replace function ${name}(`;const a=source.indexOf(marker);if(a<0)throw new Error(`FUNCTION_MISSING:${name}`);const z=source.indexOf('$function$;',source.indexOf('as $function$',a))+12;if(z<12)throw new Error('BODY_MISSING');return source.slice(a,z)+'\n';}
function replace(s,a,b){if(s.split(a).length!==2)throw new Error(`ANCHOR_NOT_EXACT:${a.slice(0,80)}`);return s.replace(a,b);}
const physical=own('02_physical_apply.sqlinc');
let live=body(physical,'private.timesheet_contract_move_evidence_v1');
live=live.replace('private.timesheet_contract_move_evidence_v1','private.timesheet_contract_move_evidence_live_v1');
live=replace(live,"'workflow_kind',w.workflow_kind,'route',w.route", "'environment',w.environment,'candidate_id',w.candidate_id,'workflow_kind',w.workflow_kind,'route',w.route");
live=replace(live,"'actual_schedule_json',t.actual_schedule_json", "'week_ending_date',t.week_ending_date,'actual_schedule_json',t.actual_schedule_json");
live=replace(live,"'immutable_submission_json',w.immutable_submission_json", "'input_snapshot_json',w.input_snapshot_json,'immutable_submission_json',w.immutable_submission_json");
live=replace(live,"'components',coalesce(","'submission_sources',private.candidate_contract_move_sources_v1(coalesce((select array_agg(w.id order by w.id) from public.candidate_submission_workflows w where w.id=t.candidate_workflow_id or w.anchor_timesheet_id=t.timesheet_id or w.target_timesheet_id=t.timesheet_id),'{}'::uuid[])),\n  'components',coalesce(");
const frozen=`create or replace function private.timesheet_contract_move_evidence_v1(p_timesheet_id uuid)
returns jsonb language plpgsql stable security definer set search_path=pg_catalog,public,private,pg_temp
as $function$
declare source_member jsonb; snapshot jsonb; a private.timesheet_contract_family_aliases%rowtype;
begin
 select v.* into a from private.timesheet_contract_family_aliases v where v.timesheet_id=p_timesheet_id;
 if not found then return private.timesheet_contract_move_evidence_live_v1(p_timesheet_id);end if;
 -- The original alias is immutable and points directly at its first accepted
 -- operation. No latest-version/chain walk or current workflow state is the seal.
 select value into source_member from private.timesheet_contract_family_aliases original_alias
  join private.timesheet_contract_move_operations o on o.id=original_alias.accepted_operation_id
  join private.timesheet_contract_move_reviews r on r.id=o.accepted_review_id and r.operation_id=o.id
  join private.timesheet_contract_move_members m on m.operation_id=o.id and m.member_key=original_alias.member_key
  cross join lateral jsonb_array_elements(r.manifest_json#>'{physical,members}') value
  where original_alias.timesheet_id=a.original_timesheet_id and original_alias.original_timesheet_id=a.original_timesheet_id
   and (o.state in('COMPLETE','SUPERSEDED') or private.timesheet_contract_move_accepting_v1(o.id)) and value->>'member_key'=a.member_key
   and value->>'original_timesheet_id'=a.original_timesheet_id::text
   and m.member_hash=private.timesheet_contract_move_row_hash_v1(value)
   and m.immutable_evidence_hash=private.timesheet_contract_move_row_hash_v1(value->'original_evidence');
 snapshot:=source_member->'original_evidence';
 if snapshot is null then raise exception 'CONTRACT_MOVE_ORIGINAL_EVIDENCE_UNBOUND' using errcode='23514';end if;
 perform private.candidate_contract_move_evidence_snapshot_assert_v1(a.original_timesheet_id,snapshot);
 return snapshot;
end;
$function$;
`;
fs.writeFileSync(path.join(here,'forward/01_evidence_capture.sqlinc'),live+'\n'+frozen);
let scope=body(physical,'private.timesheet_contract_move_scope_v1');
const old=`  select coalesce(array_agg(distinct w.id order by w.id),'{}'::uuid[]) into workflows from public.candidate_submission_workflows w
   where w.id=t.candidate_workflow_id or w.target_timesheet_id=t.timesheet_id or w.anchor_timesheet_id=t.timesheet_id;\n`;
scope=replace(scope,old,'');
scope=replace(scope,'  evidence:=private.timesheet_contract_move_evidence_v1(original_id);',`  evidence:=private.timesheet_contract_move_evidence_v1(original_id);
  -- A-anchored claim/workflow identities survive B->C. Current B-owned native
  -- workflows are added only through an exact member Timesheet association.
  select coalesce(array_agg(distinct w.id order by w.id),'{}'::uuid[]) into workflows
   from public.candidate_submission_workflows w where w.id=t.candidate_workflow_id
    or w.target_timesheet_id=t.timesheet_id or w.anchor_timesheet_id=t.timesheet_id
    or w.id in(select (value->>'id')::uuid from jsonb_array_elements(evidence->'workflows'));
  if p_lock then perform 1 from public.candidate_submission_workflows w where w.id=any(workflows) order by w.id for update;end if;`);
scope=replace(scope,"   'immutable_evidence_hash',encode", "   'original_evidence',evidence,'from_evidence',private.timesheet_contract_move_evidence_live_v1(t.timesheet_id),\n   'immutable_evidence_hash',encode");
fs.writeFileSync(path.join(here,'forward/02_scope_successor.sqlinc'),scope);
const inputs=['supabase/repeatable/10102026_1347_import_expense_qr_approval_v1.sql','supabase/repeatable/13102026_1331_candidate_finalize_next_import_expense_authority_v1.sql','supabase/repeatable/28082026_2002_candidate_daily_detail_projection_v1.sql','supabase/repeatable/27082026_1916_candidate_timesheet_summary_refresh_v1.sql','supabase/repeatable/03102026_1540_candidate_approved_hours_visible.sql','supabase/repeatable/24092026_2247_candidate_provisional_expense_carrier_lifecycle.sql','supabase/repeatable/07082026_2310_candidate_manager_review_helpers_v1.sql'];
fs.writeFileSync(path.join(here,'SOURCE_INPUTS.json'),JSON.stringify({version:1,root,files:inputs.map(p=>({path:p,sha256:hash(fs.readFileSync(path.join(root,p)))}))},null,2)+'\n');

const rootRead=p=>read(path.join(root,'supabase/repeatable',p));
let mapping=body(rootRead('28082026_2002_candidate_daily_detail_projection_v1.sql'),'private._candidate_workflow_maps_to_card_v1');
mapping=replace(mapping,'  v_parent public.timesheets%rowtype;','  v_parent public.timesheets%rowtype;\n  v_move jsonb;');
mapping=replace(mapping,'  if p_timesheet_id is not null and v_workflow.workflow_kind=\'DAILY\' then',`  if p_timesheet_id is not null and exists(select 1 from private.timesheet_contract_family_aliases a where a.timesheet_id=p_timesheet_id) then
    v_move:=private.candidate_contract_move_context_v1(v_workflow.environment,v_workflow.candidate_id,p_timesheet_id,null);
    return p_workflow_id in(select value::uuid from jsonb_array_elements_text(v_move->'workflow_ids'))
      and (p_contract_week_id is null or p_contract_week_id in((v_move->>'current_week_id')::uuid,(v_move->>'original_week_id')::uuid));
  end if;
  if p_timesheet_id is not null and v_workflow.workflow_kind='DAILY' then`);
fs.writeFileSync(path.join(here,'forward/04_workflow_card_mapping.sqlinc'),mapping);
let detail=body(rootRead('10102026_1347_import_expense_qr_approval_v1.sql'),'public.candidate_app_timesheet_detail_v1');
detail=replace(detail,'  v_display_route_label text;','  v_display_route_label text;\n  v_contract_move jsonb;\n  v_requested_timesheet_id uuid:=p_timesheet_id;\n  v_evidence_origin_id uuid;');
detail=replace(detail,"  if p_workflow_id is not null then\n    select * into v_workflow",`  if p_timesheet_id is null and p_contract_week_id is not null then
    select cw.timesheet_id into v_requested_timesheet_id from public.contract_weeks cw
      join public.contracts c on c.id=cw.contract_id and c.candidate_id=v_candidate_id where cw.id=p_contract_week_id;
  end if;
  v_contract_move:=private.candidate_contract_move_context_v1(p_environment,v_candidate_id,v_requested_timesheet_id,p_workflow_id);
  if v_contract_move is not null then
    p_timesheet_id:=(v_contract_move#>>'{dto,current_timesheet_id}')::uuid;
    p_contract_week_id:=(v_contract_move->>'current_week_id')::uuid;
    v_evidence_origin_id:=(v_contract_move#>>'{dto,evidence_origin_timesheet_id}')::uuid;
    if p_workflow_id is not null then select * into v_workflow from public.candidate_submission_workflows
      where id=p_workflow_id and candidate_id=v_candidate_id and environment=private._candidate_assert_environment(p_environment);end if;
  end if;
  if p_workflow_id is not null and v_contract_move is null then
    select * into v_workflow`);
detail=replace(detail,"   where e.timesheet_id=p_timesheet_id and e.processing_state<>'SUPERSEDED';".trimStart(),"where e.timesheet_id in(p_timesheet_id,v_evidence_origin_id) and e.processing_state<>'SUPERSEDED';");
detail=replace(detail,"where w.candidate_id=v_candidate_id and (v_daily or w.contract_id=v_contract.id)","where w.candidate_id=v_candidate_id and (v_daily or w.contract_id=v_contract.id\n      or v_contract_move is not null and w.id in(select value::uuid from jsonb_array_elements_text(v_contract_move->'workflow_ids')))");
detail=replace(detail,"    )\n  );\nend;", "    )\n  )||case when v_contract_move is null then '{}'::jsonb else jsonb_build_object('contract_move',v_contract_move->'dto') end;\nend;");
detail=replace(detail,"  v_is_expense_only:=",`  if v_contract_move#>>'{dto,is_history}'='true' then
    v_primary_action:=null;
    v_action_contract:=v_action_contract||jsonb_build_object('primary_action',null,'available_actions','[]'::jsonb,'manager_approval',null);
    v_capabilities:=v_capabilities||jsonb_build_object('can_edit_hours',false,'can_edit_expenses',false,
      'candidate_paper_submission_allowed',false,'candidate_no_work_allowed',false,
      'can_reject_candidate_submission',false,'candidate_mutation_locked',true);
  end if;
  v_is_expense_only:=`);
fs.writeFileSync(path.join(here,'forward/05_detail_successor.sqlinc'),detail);
let page=body(rootRead('24092026_2247_candidate_provisional_expense_carrier_lifecycle.sql'),'public.candidate_app_timesheet_page_v1');
page=replace(page,'where t.timesheet_id is null or (t.is_current=true and t.archived_at_utc is null)',`where (t.timesheet_id is null and not exists(select 1 from private.timesheet_contract_move_members m
      join private.timesheet_contract_move_operations accepted on accepted.id=m.operation_id and accepted.state in('COMPLETE','SUPERSEDED')
      join public.candidate_submission_workflows original_w on original_w.id=any(m.workflow_ids)
       and original_w.contract_week_id=cw.id and original_w.candidate_id=v_candidate_id))
     or (t.is_current=true and t.archived_at_utc is null)
     or (v_view='HISTORY' and t.archived_reason_code='CONTRACT_MOVE' and t.archived_at_utc is not null
       and exists(select 1 from private.timesheet_contract_family_aliases a where a.timesheet_id=t.timesheet_id))`);
page=replace(page,'    group by history.timesheet_id\n  ), expense_carriers',`      and not exists(select 1 from private.timesheet_contract_family_aliases a where a.timesheet_id=history.timesheet_id)
    group by history.timesheet_id
    union all
    select a.timesheet_id,1,(placement.value->>'current_timesheet_id')::uuid
     from private.timesheet_contract_family_aliases a
     join private.timesheet_contract_move_operations accepted on accepted.id=a.accepted_operation_id
      and accepted.candidate_id=v_candidate_id and accepted.state in('COMPLETE','SUPERSEDED')
     cross join lateral (select private.timesheet_contract_current_placement_v1(a.timesheet_id) value) placement
     join candidate_weeks current_week on current_week.timesheet_id=(placement.value->>'current_timesheet_id')::uuid
  ), expense_carriers`);
page=replace(page,"      select case\n        when base.paid_at_utc",`      select case
        when exists(select 1 from private.timesheet_contract_family_aliases a join public.timesheets old_t on old_t.timesheet_id=a.timesheet_id
          where a.timesheet_id=base.timesheet_id and old_t.archived_reason_code='CONTRACT_MOVE' and old_t.archived_at_utc is not null) then 'HISTORY'
        when base.paid_at_utc`);
fs.writeFileSync(path.join(here,'forward/06_page_successor.sqlinc'),page);
let page2=body(rootRead('03102026_1540_candidate_approved_hours_visible.sql'),'public.candidate_app_timesheet_page_v2');
page2=replace(page2,'  v_approval jsonb;','  v_approval jsonb;\n  v_context jsonb;\n  v_candidate_id uuid;\n  v_move jsonb;');
page2=replace(page2,'  for v_item in select',`  v_context:=private._candidate_session_context_v1(p_session_id,p_environment,null,p_now_utc,false);
  v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
  if v_candidate_id is null then raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000';end if;
  for v_item in select`);
page2=replace(page2,"    v_items:=v_items||pg_catalog.jsonb_build_array(v_item);",`    v_move:=private.candidate_contract_move_context_v1(p_environment,v_candidate_id,nullif(v_item->>'timesheet_id','')::uuid,null);
    if v_move is not null then v_item:=v_item||jsonb_build_object('contract_move',v_move->'dto');end if;
    if v_move#>>'{dto,is_history}'='true' then
      v_item:=v_item||jsonb_build_object('primary_action',null,
        'actions',jsonb_build_object('can_edit_hours',false,'can_edit_expenses',false,'candidate_paper_submission_allowed',false,
         'candidate_no_work_allowed',false,'can_reject_candidate_submission',false,'reject_scope',null));
    end if;
    v_items:=v_items||pg_catalog.jsonb_build_array(v_item);`);
fs.writeFileSync(path.join(here,'forward/07_page_v2_projection.sqlinc'),page2);
let touch=body(rootRead('27082026_1916_candidate_timesheet_summary_refresh_v1.sql'),'private._candidate_timesheet_summary_revision_touch_workflow_v1');
touch=replace(touch,'  v_current_timesheet_id uuid;','  v_current_timesheet_id uuid;\n  v_move jsonb;\n  v_current_week_id uuid;');
touch=replace(touch,'  if v_workflow.contract_week_id is not null then',`  if exists(select 1 from private.timesheet_contract_family_aliases a where a.timesheet_id=v_current_timesheet_id) then
    v_move:=private.candidate_contract_move_context_v1(v_workflow.environment,v_workflow.candidate_id,v_current_timesheet_id,null);
    v_current_timesheet_id:=(v_move#>>'{dto,current_timesheet_id}')::uuid;
    v_current_week_id:=(v_move->>'current_week_id')::uuid;
    perform private._candidate_timesheet_summary_revision_upsert_v1('TIMESHEET',v_current_timesheet_id,v_current_timesheet_id,v_current_week_id);
    perform private._candidate_timesheet_summary_revision_upsert_v1('CONTRACT_WEEK',v_current_week_id,v_current_timesheet_id,v_current_week_id);
  end if;
  if v_workflow.contract_week_id is not null then`);
fs.writeFileSync(path.join(here,'forward/08_summary_touch_successor.sqlinc'),touch);

let transition=body(rootRead('10102026_1347_import_expense_qr_approval_v1.sql'),'public.candidate_workflow_transition_atomic_v1');
transition=replace(transition,'  v_replaced_manager_signature_ids uuid[]:=array[]::uuid[];','  v_replaced_manager_signature_ids uuid[]:=array[]::uuid[];\n  v_contract_move jsonb;');
transition=replace(transition,"      return v_mutation_receipt->'response';",`      if v_action='BEGIN_MANAGER_REVIEW' then
        v_contract_move:=private.candidate_contract_move_context_v1(v_environment,v_workflow.candidate_id,null,v_workflow.id);
        return (v_mutation_receipt->'response')||case when v_contract_move is null then '{}'::jsonb
          else jsonb_build_object('contract_move',v_contract_move->'dto') end;
      end if;
      return v_mutation_receipt->'response';`);
transition=replace(transition,"      'can_approve',true,'can_refuse',true);",`      'can_approve',true,'can_refuse',true);
    -- This association is derived only after the genuine manager token/request/
    -- generation/manifest/expiry proof above. It grants no Candidate authority.
    v_contract_move:=private.candidate_contract_move_context_v1(v_environment,v_workflow.candidate_id,null,v_workflow.id);
    if v_contract_move is not null then v_response:=v_response||jsonb_build_object('contract_move',v_contract_move->'dto');end if;`);
fs.writeFileSync(path.join(here,'forward/09_manager_start_successor.sqlinc'),transition);
let summaryTrigger=body(rootRead('27082026_1916_candidate_timesheet_summary_refresh_v1.sql'),'private._candidate_timesheet_summary_workflow_revision_trg_v1');
summaryTrigger=replace(summaryTrigger,"  if tg_op='DELETE' then return old; end if;", "  if tg_op<>'DELETE' then perform private._candidate_timesheet_summary_revision_touch_workflow_v1(new.id);end if;\n  if tg_op='DELETE' then return old; end if;");
fs.writeFileSync(path.join(here,'forward/10_workflow_revision_trigger_successor.sqlinc'),summaryTrigger);
const broker=read(path.join(root,'broker/src/candidate-app-backend.js'));
function jsFunction(s,name){let a=s.indexOf(`async function ${name}(`);if(a<0)a=s.indexOf(`function ${name}(`);if(a<0)throw new Error(`JS_FUNCTION_MISSING:${name}`);let b=s.indexOf('\n}\n',a);if(b<0)throw new Error('JS_FUNCTION_END_MISSING');return s.slice(a,b+3);}
let managerResult=jsFunction(broker,'managerStartResult');
managerResult=replace(managerResult,'  return {\n    ok: true,',`  const move = result?.contract_move == null ? null : decodeCandidateContractMove(result.contract_move);
  return {
    ...(move ? { contract_move: move } : {}),
    ok: true,`);
let paperContext=jsFunction(broker,'candidatePaperPackContext');
paperContext=replace(paperContext,"  const id = requireUuid(timesheetId, 'CANDIDATE_TIMESHEET_NOT_FOUND');", "  const requestedId = requireUuid(timesheetId, 'CANDIDATE_TIMESHEET_NOT_FOUND');\n  let id = requestedId;");
paperContext=replace(paperContext,"  await rpcCall(deps, 'candidate_app_timesheet_detail_v1', candidateRpcArgs(access, env, {\n    p_timesheet_id: id,", "  const detail = await rpcCall(deps, 'candidate_app_timesheet_detail_v1', candidateRpcArgs(access, env, {\n    p_timesheet_id: requestedId,");
paperContext=replace(paperContext,"  const timesheet = await restOne(env, 'timesheets',",`  const move = detail?.contract_move == null ? null : decodeCandidateContractMove(detail.contract_move);
  if (move) {
    if (![move.original_timesheet_id, move.current_timesheet_id].includes(requestedId) && move.is_history !== true) {
      throw new CandidateHttpError(409, 'CANDIDATE_CONTEXT_STALE');
    }
    id = move.evidence_origin_timesheet_id;
  }
  // A moved original is an immutable source. The preceding native detail proof
  // authorises this exact source ID; ordinary requests retain the current-only read.
  const timesheet = await restOne(env, 'timesheets',`);
paperContext=replace(paperContext,"`timesheet_id=eq.${encodeURIComponent(id)}&is_current=eq.true`", "`timesheet_id=eq.${encodeURIComponent(id)}${move ? '' : '&is_current=eq.true'}`");
fs.writeFileSync(path.join(here,'forward/11_broker_native_contexts.mjsinc'),managerResult+'\n\n'+paperContext+'\n');
fs.writeFileSync(path.join(here,'BROKER_INPUT.json'),JSON.stringify({path:'broker/src/candidate-app-backend.js',sha256:hash(fs.readFileSync(path.join(root,'broker/src/candidate-app-backend.js'))),completeFunctions:['managerStartResult','candidatePaperPackContext'],import:'decodeCandidateContractMove from candidate-contract-move.mjs'},null,2)+'\n');
console.log(JSON.stringify({written:fs.readdirSync(path.join(here,'forward')),sourceOnly:true}));


