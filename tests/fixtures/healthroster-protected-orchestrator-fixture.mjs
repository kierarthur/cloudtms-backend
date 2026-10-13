// Synthetic fixture copied from the current native orchestrator suite; no hosted facts.
const ID = Object.freeze({
  actor: '91000000-0000-4000-8000-000000000001',
  agency: '91000000-0000-4000-8000-000000000002',
  cycle: '91000000-0000-4000-8000-000000000003',
  group: '91000000-0000-4000-8000-000000000004',
  candidate: '91000000-0000-4000-8000-000000000005',
  client: '91000000-0000-4000-8000-000000000006',
  contract: '91000000-0000-4000-8000-000000000007',
  week: '91000000-0000-4000-8000-000000000008',
  root: '91000000-0000-4000-8000-000000000009',
  event: '91000000-0000-4000-8000-00000000000a',
  family: '91000000-0000-4000-8000-00000000000b',
  run: '91000000-0000-4000-8000-00000000000c',
  financial: '91000000-0000-4000-8000-00000000000d',
  source: '91000000-0000-4000-8000-00000000000e',
  expense: '91000000-0000-4000-8000-00000000000f',
});
const HASH = 'a'.repeat(64);

const schedule = Object.freeze({
  work_date: '2026-09-14',
  start_at_local: '2026-09-14 09:00:00',
  end_at_local: '2026-09-14 17:00:00',
  break_minutes: 30,
});

function browserRequest(action, overrides = {}) {
  const base = {
    actor_user_id: ID.actor,
    source_cycle_id: ID.cycle,
    work_event_id: ID.event,
    reason: 'Office approved the candidate hours.',
    idempotency_key: `weekly-protected-${action.toLowerCase()}-0001`,
  };
  if (action === 'APPROVE_PROTECTED_HOURS') {
    return {
      ...base,
      candidate_id: ID.candidate,
      client_id: ID.client,
      contract_id: ID.contract,
      week_ending_date: '2026-09-20',
      work_date: '2026-09-14',
      start: '09:00',
      end: '17:00',
      break_minutes: 30,
      evidence_timesheet_id: null,
      ...overrides,
    };
  }
  const existing = {
    ...base,
    family_id: ID.family,
    expected_family_bound_version: '7',
  };
  if (action === 'AMEND_PROTECTED_HOURS') {
    Object.assign(existing, {
      work_date: '2026-09-14', start: '08:00', end: '18:00', break_minutes: 60,
      evidence_timesheet_id: null,
    });
  }
  return { ...existing, ...overrides };
}

function prepared(kind, protectedSchedule = schedule) {
  return {
    ok: true,
    outcome: 'PREPARED',
    family_id: ID.family,
    orchestration_run_id: ID.run,
    source_cycle_id: ID.cycle,
    source_group_id: ID.group,
    agency_id: ID.agency,
    candidate_id: ID.candidate,
    client_id: ID.client,
    contract_id: ID.contract,
    contract_week_id: ID.week,
    root_timesheet_id: ID.root,
    work_event_id: ID.event,
    week_ending_date: '2026-09-20',
    family_bound_version: '7',
    request_kind: kind,
    protected_schedule: protectedSchedule,
    run_state: 'RUNNING',
  };
}

function context(kind, {
  sourcePresent = true,
  financial = true,
  sourceExpense = true,
  protectedScheduleOverride = null,
} = {}) {
  const protectedSchedule = protectedScheduleOverride ?? (kind === 'AMEND'
    ? { ...schedule, start_at_local: '2026-09-14 08:00:00', end_at_local: '2026-09-14 18:00:00', break_minutes: 60 }
    : schedule);
  const decisionState = ['APPROVE', 'AMEND', 'WAIT'].includes(kind)
    ? 'WAIT'
    : ['WITHDRAW', 'RECONCILE'].includes(kind) ? 'ACCEPTED_SOURCE' : 'NOT_WORKED';
  const sourceSegments = sourcePresent ? [{
    work_event_id: ID.event,
    date: '2026-09-14',
    start: '09:00',
    end: '17:00',
    break_mins: 30,
  }] : [];
  return {
    ok: true,
    contract: 'WEEKLY_PROTECTED_ACTION_CONTEXT_V1',
    action: kind,
    family_id: ID.family,
    orchestration_run_id: ID.run,
    family_bound_version: '7',
    current_target_vector_sha256: HASH,
    source_cycle_id: ID.cycle,
    source_group_id: ID.group,
    agency_id: ID.agency,
    candidate_id: ID.candidate,
    client_id: ID.client,
    contract_id: ID.contract,
    contract_week_id: ID.week,
    root_timesheet_id: ID.root,
    work_event_id: ID.event,
    week_ending_date: '2026-09-20',
    source_mode: 'HEALTHROSTER_WEEKLY',
    policy: { authority_mode: 'SOURCE_AUTHORITY', erni_pct: 0, apply_erni_to: 'PAYE_ONLY' },
    timesheet: {
      timesheet_id: ID.root, version: 1, contract_id: ID.contract,
      week_ending_date: '2026-09-20', actual_schedule_json: [],
    },
    contract_week: { id: ID.week, contract_id: ID.contract, week_ending_date: '2026-09-20' },
    contract_record: {
      id: ID.contract, candidate_id: ID.candidate, client_id: ID.client,
      role: 'Nurse', band: 'Band 5', pay_method_snapshot: 'PAYE',
      rates_json: {
        pay: { day: 10, night: 10, sat: 10, sun: 10, bh: 10 },
        charge: { day: 20, night: 20, sat: 20, sun: 20, bh: 20 },
      },
    },
    current_financial: financial ? { id: ID.financial, timesheet_id: ID.root, is_current: true } : null,
    requires_zero_financial: !financial,
    protected_schedule: protectedSchedule,
    source_segments: sourceSegments,
    protected_decisions: [{
      work_event_id: ID.event,
      state: decisionState,
      fixed_schedule: decisionState === 'WAIT' ? {
        date: protectedSchedule.work_date,
        start: protectedSchedule.start_at_local.slice(11, 16),
        end: protectedSchedule.end_at_local.slice(11, 16),
        break_mins: protectedSchedule.break_minutes,
      } : null,
    }],
    source_proposal: {
      selected_work_event_id: ID.event,
      source_present: sourcePresent,
      source_minutes: sourcePresent ? 450 : 0,
      source_revision: ID.cycle,
      source_hash: HASH,
      source_segments: sourceSegments,
    },
    client_sources: [{
      source_system: 'CLOUDTMS_WEEKLY_FINAL_SOURCE',
      external_identity: ID.event,
      external_revision: ID.cycle,
      document_sha256: HASH,
      work_date: '2026-09-14',
      client_source_id: ID.source,
      source_complete: true,
      source_present: sourcePresent,
      approved_minutes: sourcePresent ? 450 : 0,
    }],
    source_expenses: sourceExpense ? [{
      expense_code: `SOURCE_SUPPLIED:${ID.event}`,
      authority_kind: 'SOURCE_EXPENSE',
      source_expense_id: ID.expense,
      document_sha256: HASH,
      pay_ex_vat: '5.00',
      charge_ex_vat: '5.00',
    }] : [],
    candidate_submission: null,
    current_comparison_revision_id: null,
    current_final_revision_id: null,
    root_financial: financial ? {
      source_system: 'CLOUDTMS_TIMESHEET_FINANCIAL',
      external_identity: ID.financial,
      external_revision: '1',
      document_sha256: HASH,
      financial_row_id: ID.financial,
      root_version: 1,
      financial_timesheet_version: 1,
      financial_revision_digest: HASH,
    } : null,
    provider: {
      source_system: 'CLOUDTMS_CONTRACT_PROVIDER',
      external_identity: ID.contract,
      external_revision: '1',
      document_sha256: HASH,
      source_pay_method: 'PAYE', umbrella_id: null,
      provider_authority_sha256: HASH,
      target_pay_method: 'PAYE', target_umbrella_id: null,
      target_enabled: true, target_vat_chargeable: null,
    },
    expected_head_revision: '0',
    request_sequence: '1',
    zero_rate_source_refs: {
      schema_version: 'WEEKLY_SOURCE_TARGET_MANAGED_ZERO_RATE_SOURCE_V1',
      source_cycle_id: ID.cycle, source_group_id: ID.group,
      source_family: 'CONFIGURABLE', source_mode: 'HEALTHROSTER_WEEKLY',
      source_profile_domain: 'ROSTER_FINAL_AUTHORITY', target_family_id: ID.family,
      root_timesheet_id: ID.root, effective_policy_sha256: HASH,
    },
  };
}

function financialSnapshot(targetSchedule, { sourceExpense = true } = {}) {
  const segments = targetSchedule.map((row, index) => ({
    date: row.date,
    segment_id: `segment-${index + 1}`,
    segment_key: `segment-key-${index + 1}`,
    segment_stable_key: row.work_event_id,
    hours_day: 7.5,
    hours_night: 0,
    hours_sat: 0,
    hours_sun: 0,
    hours_bh: 0,
    pay_amount: 75,
    charge_amount: 150,
    exclude_from_pay: false,
  }));
  return {
    timesheet_id: ID.root,
    timesheet_version: 1,
    candidate_id: ID.candidate,
    client_id: ID.client,
    role: 'Nurse', band: 'Band 5', pay_method: 'PAYE',
    pay_day: 10, pay_night: 10, pay_sat: 10, pay_sun: 10, pay_bh: 10,
    charge_day: 20, charge_night: 20, charge_sat: 20, charge_sun: 20, charge_bh: 20,
    hours_day: segments.length * 7.5, hours_night: 0, hours_sat: 0, hours_sun: 0, hours_bh: 0,
    total_hours: segments.length * 7.5,
    additional_units_json: {}, additional_pay_ex_vat: 0,
    additional_charge_ex_vat: 0, additional_margin_ex_vat: 0,
    expenses_pay_ex_vat: sourceExpense ? 5 : 0,
    expenses_charge_ex_vat: sourceExpense ? 5 : 0,
    mileage_units: 0, mileage_pay_ex_vat: 0, mileage_charge_ex_vat: 0,
    total_pay_ex_vat: segments.length * 75 + (sourceExpense ? 5 : 0),
    total_charge_ex_vat: segments.length * 150 + (sourceExpense ? 5 : 0),
    margin_ex_vat: segments.length * 75,
    invoice_breakdown_json: {
      mode: 'SEGMENTS',
      segments,
      additional: { units: {}, pay_ex_vat: 0, charge_ex_vat: 0, margin_ex_vat: 0 },
      totals: {
        total_pay_ex_vat: segments.length * 75 + (sourceExpense ? 5 : 0),
        total_charge_ex_vat: segments.length * 150 + (sourceExpense ? 5 : 0),
        margin_ex_vat: segments.length * 75,
      },
    },
  };
}

function dependenciesFor(kind, options = {}) {
  const calls = [];
  let stagedRequest = null;
  let contextReads = 0;
  const preparedKind = options.preparedKind ?? kind;
  const sourcePresent = options.sourcePresent ?? kind !== 'RECORD_NOT_WORKED';
  const deps = {
    calls,
    dataRpc: async (name, args) => {
      calls.push([name, args]);
      if (name === 'weekly_exceptional_pay_prepare_family_v1') return options.preparedResult ?? prepared(preparedKind);
      if (name === 'weekly_exceptional_pay_prepare_action_v1') {
        const protectedSchedule = kind === 'AMEND'
          ? args.p_request.protected_schedule
          : schedule;
        return options.preparedResult ?? prepared(kind, protectedSchedule);
      }
      if (name === 'weekly_exceptional_pay_action_publication_status_v1') {
        return options.publicationStatus ?? { ok: true, staged: false };
      }
      if (name === 'weekly_exceptional_pay_action_context_v1') {
        contextReads += 1;
        return context(preparedKind, {
          sourcePresent,
          financial: options.zeroRoot ? contextReads > 1 : true,
          sourceExpense: options.sourceExpense ?? true,
          protectedScheduleOverride: options.preparedKind ? schedule : null,
        });
      }
      if (name === 'weekly_source_target_managed_root_prepare_atomic_v1') {
        return { ok: true, outcome: 'PREPARED' };
      }
      if (name === 'weekly_exceptional_pay_wait_atomic_v1') {
        return { ok: true, outcome: 'WAITING_FOR_SOURCE' };
      }
      if (name === 'weekly_exceptional_pay_stage_c1_request_v1') {
        stagedRequest = args.p_request;
        return {
          ok: true, outcome: 'STAGED',
          publication_request_id: stagedRequest.c1_request.request_id,
          request_sha256: stagedRequest.c1_request.request_sha256,
        };
      }
      if (name === 'weekly_exceptional_pay_complete_local_v1') {
        return options.localResult ?? { ok: true, outcome: 'PUBLISHED', state: 'LIVE' };
      }
      throw new Error(`Unexpected data RPC ${name}`);
    },
    calculateWeeklySnapshot: async ({ schedule: targetSchedule }) => {
      calls.push(['CALCULATE', targetSchedule]);
      return { snapshot: financialSnapshot(targetSchedule, { sourceExpense: options.sourceExpense ?? true }) };
    },
    c1RawRpc: async () => {
      calls.push(['C1_RAW']);
      throw new Error('A published durable fixture must not call C1.');
    },
    staged: () => stagedRequest,
  };
  return deps;
}


export { ID, HASH, browserRequest, prepared, dependenciesFor };
