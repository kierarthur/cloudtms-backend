import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url));fs.mkdirSync(path.join(here,'forward'),{recursive:true});
const read=p=>fs.readFileSync(path.resolve(here,p),'utf8').replace(/\r\n/g,'\n');
const replace=(s,a,b)=>{if(s.split(a).length!==2)throw Error('ANCHOR_NOT_EXACT:'+a.slice(0,80));return s.replace(a,()=>b);};
let detail=read('../candidate-native-pending-20261016/forward/11_candidate_detail.sqlinc');
detail=replace(detail,"and v_workflow.state in ('DRAFT','WORKER_SUBMITTED'","and v_workflow.state in ('DRAFT','WORKER_DRAFT','WORKER_SUBMITTED'");
fs.writeFileSync(path.join(here,'forward/00_expense_draft_policy.sqlinc'),detail);
let worker=read('../candidate-contract-move-lifecycle-20261016/forward/02_weekly_current_calculation.mjsinc');
worker=replace(worker,`      calculationWorkflow = { ...workflow, contract_id: week.contract_id, contract_week_id: week.id,
        target_timesheet_id: move.current_mutable_timesheet_id, policy_snapshot_json: detail.capabilities.policy, _contract_move_current: true };`,`      if (workflow.workflow_kind === 'CONTRACT_EXPENSE') {
        // The worked anchor and server-resolved expense carrier are different
        // native identities. A targetless draft stays targetless; the existing
        // canonical producer retains its normal prospective create payload.
        let calculationWeek = week;
        let calculationTarget = null;
        if (workflow.target_timesheet_id) {
          const carrier = await rpcCall(deps, 'candidate_app_timesheet_detail_v1', candidateRpcArgs(access, env, {
            p_timesheet_id: workflow.target_timesheet_id, p_contract_week_id: null, p_workflow_id: null
          }));
          const carrierMove = decodeCandidateContractMove(carrier?.contract_move);
          if (carrierMove.family_root_id !== move.family_root_id || carrierMove.operation_id !== move.operation_id
              || carrier.timesheet?.id !== carrierMove.current_mutable_timesheet_id
              || carrier.contract_week?.contract_id !== week.contract_id || !UUID_RE.test(text(carrier.contract_week?.id))
              || !(Array.isArray(carrier.workflows) ? carrier.workflows : []).some(w =>
                w.workflow_id === workflow.id && Number(w.generation) === Number(workflow.generation))) {
            throw new CandidateHttpError(409, 'CANDIDATE_CONTEXT_STALE');
          }
          calculationWeek = carrier.contract_week;
          calculationTarget = carrierMove.current_mutable_timesheet_id;
        }
        calculationWorkflow = { ...workflow, contract_id: week.contract_id, contract_week_id: calculationWeek.id,
          target_timesheet_id: calculationTarget, anchor_timesheet_id: move.current_mutable_timesheet_id,
          policy_snapshot_json: detail.capabilities.policy, _contract_move_current: calculationTarget != null,
          _contract_move_expense_anchor: { timesheet_id: move.current_mutable_timesheet_id, contract_week_id: week.id } };
      } else {
        calculationWorkflow = { ...workflow, contract_id: week.contract_id, contract_week_id: week.id,
          target_timesheet_id: move.current_mutable_timesheet_id, policy_snapshot_json: detail.capabilities.policy, _contract_move_current: true };
      }`);
worker=replace(worker,'  const schedule = Array.isArray(factual.actual_schedule_json)',`  if (workflow._contract_move_expense_anchor) {
    const anchor = workflow._contract_move_expense_anchor;
    const [anchorTimesheet, anchorWeek] = await Promise.all([
      sbGetOne(env, \`\${env.SUPABASE_URL}/rest/v1/timesheets?timesheet_id=eq.\${enc(anchor.timesheet_id)}&is_current=eq.true&select=*\`),
      String(anchor.contract_week_id) === String(cw.id) ? Promise.resolve(cw)
        : sbGetOne(env, \`\${env.SUPABASE_URL}/rest/v1/contract_weeks?id=eq.\${enc(anchor.contract_week_id)}&select=*\`)
    ]);
    if (workflow.workflow_kind !== 'CONTRACT_EXPENSE' || !anchorTimesheet || !anchorWeek
        || String(anchorTimesheet.timesheet_id) !== String(anchor.timesheet_id)
        || String(anchorWeek.id) !== String(anchor.contract_week_id)
        || anchorTimesheet.is_current !== true || anchorTimesheet.archived_at_utc != null
        || String(anchorTimesheet.contract_id) !== String(contract.id) || anchorTimesheet.sheet_scope !== 'WEEKLY'
        || ['EXPENSES', 'MILEAGE'].includes(String(anchorTimesheet.line_type || '').toUpperCase())
        || String(anchorWeek.contract_id) !== String(contract.id) || String(anchorWeek.timesheet_id) !== String(anchor.timesheet_id)
        || String(anchorWeek.week_ending_date) !== String(cw.week_ending_date)
        || (workflow.target_timesheet_id && (!existingTimesheet
          || !['EXPENSES', 'MILEAGE'].includes(String(existingTimesheet.line_type || '').toUpperCase())
          || Number(cw.additional_seq) <= 0))) {
      throw new Error('CANDIDATE_CONTEXT_STALE');
    }
  }
  const schedule = Array.isArray(factual.actual_schedule_json)`);
fs.writeFileSync(path.join(here,'forward/01_current_expense_preparation.mjsinc'),worker);
console.log('Complete detail policy and actual canonical preparation functions generated; native moved expense submit remains held.');
