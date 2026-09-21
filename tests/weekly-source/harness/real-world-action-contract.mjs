import { deepFreeze } from './canonical-json.mjs';

// This is an execution contract, not evidence.  It prevents a populated
// scenario from being treated as proved by the parser-only test.  Stage 2 must
// execute every listed action through these product owners on both NEW and
// UPGRADE PostgreSQL 17.11 databases before either real-world scenario can
// write a PASS envelope.
export const REAL_WORLD_DATABASE_MODES = deepFreeze(['NEW', 'UPGRADE']);

export const REAL_WORLD_ACTION_OWNERS = deepFreeze({
  UPLOAD_SOURCE: {
    layer: 'DATABASE_AND_SERVICE',
    owners: [
      'broker/src/weekly-source/index.js#parseWeeklySourceFile',
      'public.weekly_source_upload_stage_begin_atomic_v1(jsonb)',
      'public.weekly_source_upload_stage_rows_atomic_v1(jsonb)',
      'public.weekly_source_upload_seal_atomic_v1(jsonb)',
      'public.weekly_source_projection_begin_atomic_v1(jsonb)',
      'public.weekly_source_projection_rows_apply_atomic_v1(uuid,uuid,jsonb)',
      'public.weekly_source_projection_publish_atomic_v1(jsonb)',
    ],
    observes: ['upload', 'parsed_rows', 'projection_rows'],
  },
  CONFIRM_CURRENT_UPLOAD: {
    layer: 'DATABASE',
    owners: [
      'public.weekly_source_upload_seal_atomic_v1(jsonb)',
      'public.weekly_source_projection_publish_atomic_v1(jsonb)',
    ],
    observes: ['current_upload', 'current_projection'],
  },
  PROTECT_HOURS: {
    layer: 'DATABASE',
    owners: [
      'public.weekly_exceptional_pay_prepare_family_v1(jsonb)',
      'public.weekly_exceptional_pay_prepare_action_v1(jsonb)',
    ],
    observes: ['protected_family', 'protected_schedule', 'candidate_entitlement'],
  },
  FINALISE: {
    layer: 'DATABASE',
    owners: ['public.weekly_source_finalise_atomic_v1(jsonb)'],
    observes: ['final_revision', 'source_movements', 'billing_movements', 'source_manifest'],
  },
  AUTHORISE: {
    layer: 'DATABASE_AND_SERVICE',
    owners: [
      'public.weekly_source_first_authorise_v1(uuid,uuid,text,uuid)',
      'broker/src/weekly-source/authorise-routing.mjs',
    ],
    observes: ['authorisation_generation', 'timesheet_lifecycle', 'approved_hours'],
  },
  CREATE_INVOICE_BATCH: {
    layer: 'DATABASE_AND_SERVICE',
    owners: [
      'public.weekly_source_invoice_batch_candidates_v1(jsonb)',
      'public.weekly_source_invoice_batch_admit_atomic_v1(jsonb)',
      'broker/src/weekly-source/invoice-batch-integration.mjs',
    ],
    observes: ['invoice_lines', 'invoice_totals', 'backing_report_number'],
  },
  RECONCILE_PROTECTED_HOURS: {
    layer: 'DATABASE',
    owners: [
      'public.weekly_exceptional_pay_prepare_action_v1(jsonb)',
      'public.weekly_exceptional_pay_action_publication_status_v1(jsonb)',
    ],
    observes: ['protected_reconciliation', 'candidate_entitlement', 'audit_events'],
  },
  READ_PROJECTION: {
    layer: 'DATABASE_AND_SERVICE',
    owners: [
      'public.weekly_source_office_timesheet_presentation_v1(jsonb)',
      'public.weekly_source_timesheet_audit_chronology_v1(jsonb)',
      'public.weekly_source_timesheet_hours_export_v1(jsonb)',
    ],
    observes: ['source_hours', 'submitted_hours', 'approved_hours', 'audit_events'],
  },
});

export const REAL_WORLD_REQUIRED_OBSERVATIONS = deepFreeze([
  'sourceMovements',
  'invoiceLines',
  'approvedHours',
  'communications',
  'audit',
  'forbiddenOutcomes',
]);

export function buildRealWorldActionExecutionPlan(scenario) {
  if (!scenario?.tags?.includes('REAL_WORLD')) {
    throw new TypeError('A validated REAL_WORLD scenario is required');
  }
  const actions = scenario.actions.map((action, index) => {
    const contract = REAL_WORLD_ACTION_OWNERS[action.kind];
    if (!contract) throw new TypeError(`No real product owner is registered for ${action.kind}`);
    return deepFreeze({
      actionNumber: index + 1,
      kind: action.kind,
      atUtc: action.atUtc ?? null,
      ownerLayer: contract.layer,
      owners: contract.owners,
      observes: contract.observes,
      action,
    });
  });
  return deepFreeze({
    scenarioId: scenario.scenarioId,
    modes: REAL_WORLD_DATABASE_MODES,
    actions,
    requiredObservations: REAL_WORLD_REQUIRED_OBSERVATIONS,
    parserOnlyCanPass: false,
    expectedMayBeCopiedToActual: false,
  });
}

