import { decodeSourceCurrentFinancialSource, decodeSourceProtectedCurrentPlacement } from './source-native-contract.mjs';

// Original root_financial/evidence A is never replaced in the native context.
// Only the real C1 stream writer receives this separately read B source fact.
export async function loadWeeklyProtectedCurrentFinancialSource({ context, actorUserId, dataRpc }) {
  if (context?.protected_current_placement?.moved !== true) return null;
  const placement = decodeSourceProtectedCurrentPlacement(context.protected_current_placement, {
    family_id: context.family_id, original_root_timesheet_id: context.root_timesheet_id });
  if (typeof dataRpc !== 'function' || placement.current_financial_id === null) {
    const error = new Error('SOURCE_CURRENT_FINANCIAL_SOURCE_REQUIRED');error.code = error.message;error.status = 409;throw error;
  }
  const result = decodeSourceCurrentFinancialSource(await dataRpc('weekly_source_protected_current_financial_source_v1', { p_request: {
    version: 'SOURCE_CURRENT_FINANCIAL_SOURCE_REQUEST_V1', actor_user_id: actorUserId, family_id: context.family_id,
    work_event_id: context.work_event_id, source_cycle_id: context.source_cycle_id,
    expected_placement_certificate_sha256: placement.placement_certificate_sha256,
  } }), { family_id: context.family_id, work_event_id: context.work_event_id, source_cycle_id: context.source_cycle_id,
    placement_certificate_sha256: placement.placement_certificate_sha256, current_timesheet_id: placement.current_timesheet_id,
    current_contract_id: placement.current_contract_id, financial_row_id: placement.current_financial_id, root_version: placement.current_timesheet_version });
  return result.root_financial;
}
