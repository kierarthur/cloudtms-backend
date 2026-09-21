import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';

import { parseWeeklySourceFile, WEEKLY_SOURCE_PROFILE_IDS } from '../../../broker/src/weekly-source/index.js';
import { createWeeklySourceUploadPublicationOwner } from '../../../broker/src/weekly-source/upload-publication-owner.mjs';
import {
  admitWeeklySourceInvoiceBatch,
  loadWeeklySourceInvoiceBatchCandidates,
  preflightWeeklySourceInvoiceBatch,
  WEEKLY_SOURCE_INVOICE_BATCH_SELECTION_CONTRACT,
} from '../../../broker/src/weekly-source/invoice-batch-integration.mjs';
import { canonicalDigest, cloneJson, deepFreeze } from '../harness/canonical-json.mjs';
import { buildExpectedSourceModel } from '../harness/expected-source-model.mjs';
import { createLocalPostgrest } from '../wp23-postgrest-local.mjs';

const FIXED_EXPENSE_PROFILE = deepFreeze({
  profileId: 'SOURCE_FIXED_EXPENSE_WHOLE_SHIFT_CSV_V1',
  defaultRateFamily: 'STD',
  unitCostByRateFamilyPence: { STD: '2000' },
  businessHierarchy: {
    grandParent: 'Scenario group',
    parent: 'Scenario parent',
    unit: 'Scenario unit',
  },
  agencyDisplayName: 'Scenario Agency',
});

function fail(code, message, details = {}) {
  throw Object.assign(new Error(message), { code, details });
}

function repositoryReadback() {
  const runGit = (args) => {
    const result = spawnSync('git', args, { cwd: process.cwd(), encoding: 'utf8', windowsHide: true });
    if (result.error || result.status !== 0) fail('WEEKLY_SOURCE_REAL_WORLD_GIT_EVIDENCE_FAILED', 'Local repository evidence could not be read.');
    return String(result.stdout ?? '').trim();
  };
  const commit = runGit(['rev-parse', 'HEAD']);
  if (!/^[a-f0-9]{40}$/.test(commit)) fail('WEEKLY_SOURCE_REAL_WORLD_GIT_EVIDENCE_INVALID', 'Local repository commit evidence is invalid.');
  return { repository: 'cloudtms-backend', commit, dirty: runGit(['status', '--porcelain']).length > 0 };
}

function uuid(seed) {
  const bytes = Buffer.from(createHash('sha256').update(String(seed)).digest().subarray(0, 16));
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = bytes.toString('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function literal(value) {
  if (value === null || value === undefined) return 'null';
  const text = String(value);
  if (text.includes('$ws$')) fail('WEEKLY_SOURCE_TEST_LITERAL_INVALID', 'A fixture value collides with the SQL test tag.');
  return `$ws$${text}$ws$`;
}

function json(value) {
  return `${literal(JSON.stringify(value))}::jsonb`;
}

function databasePort(connectionUrl) {
  const parsed = new URL(connectionUrl);
  if (parsed.protocol !== 'postgresql:' || parsed.hostname !== '127.0.0.1') {
    fail('WEEKLY_SOURCE_REAL_WORLD_DATABASE_REFUSED', 'The real-world adapter accepts only a local PostgreSQL target.');
  }
  const port = Number.parseInt(parsed.port, 10);
  if (!Number.isInteger(port) || port < 1024 || port > 65535) {
    fail('WEEKLY_SOURCE_REAL_WORLD_DATABASE_REFUSED', 'The local PostgreSQL port is invalid.');
  }
  return port;
}

function withDatabase(connectionUrl, databaseName) {
  if (!/^[a-z][a-z0-9_]{0,62}$/.test(databaseName)) {
    fail('WEEKLY_SOURCE_REAL_WORLD_DATABASE_REFUSED', 'The disposable database name is invalid.');
  }
  const parsed = new URL(connectionUrl);
  parsed.pathname = `/${databaseName}`;
  return parsed.toString();
}

function executePsql(database, sql, { tuples = true } = {}) {
  const args = ['-X', '-v', 'ON_ERROR_STOP=1', database.connectionUrl];
  if (tuples) args.push('-tA');
  args.push('-c', sql);
  const result = spawnSync(database.psqlBin || process.env.PSQL_BIN || 'psql', args, {
    encoding: 'utf8',
    env: { ...process.env, PGPASSWORD: process.env.PGPASSWORD || 'localonly' },
    maxBuffer: 128 * 1024 * 1024,
  });
  if (result.status !== 0) {
    fail('WEEKLY_SOURCE_REAL_WORLD_SQL_FAILED', String(result.stderr || result.stdout).trim().slice(0, 2000));
  }
  return String(result.stdout || '').trim();
}

function queryJson(database, sql) {
  const output = executePsql(database, `select coalesce(pg_catalog.jsonb_agg(q),'[]'::jsonb)::text from (${sql}) q;`);
  return JSON.parse(output || '[]');
}

function parseProfile(upload) {
  if (upload.profile === 'NHSP_PREFINAL_RELEASED_V1') return WEEKLY_SOURCE_PROFILE_IDS.NHSP_PREFINAL_RELEASED_V1;
  if (upload.profile === 'NHSP_FINAL_BACKING_V1') return WEEKLY_SOURCE_PROFILE_IDS.NHSP_FINAL_BACKING_V1;
  if (upload.profile.endsWith('LAYOUT_A_V1')) return WEEKLY_SOURCE_PROFILE_IDS.HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1;
  if (upload.profile.endsWith('LAYOUT_B_V1')) return WEEKLY_SOURCE_PROFILE_IDS.HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1;
  if (upload.profile === 'GENERIC_WEEKLY_COMPLETE_V1') return WEEKLY_SOURCE_PROFILE_IDS.ROSTER_WEEKLY_SUMMARY_ACTUAL_V1;
  fail('WEEKLY_SOURCE_REAL_WORLD_PROFILE_UNSUPPORTED', `No production parser mapping exists for ${upload.profile}.`);
}

function parserOptions(upload) {
  return {
    profileId: parseProfile(upload),
    ...(upload.profile === 'NHSP_FINAL_BACKING_V1' ? {
      configuredNhspReportHeadingName: 'Scenario Agency',
      expectedTrust: upload.trustName,
    } : {}),
    ...(!upload.profile.startsWith('NHSP_') ? { expectedClient: upload.trustName } : {}),
  };
}

function parsedMinutes(row) {
  return row.actual?.totalMinutes ?? row.wholeShiftInputs?.paidMinutes ?? null;
}

function parsedStart(row) {
  return row.actual?.start ?? row.wholeShiftInputs?.bookingStartLocal?.slice(11, 16) ?? null;
}

function parsedEnd(row) {
  return row.actual?.end ?? row.wholeShiftInputs?.bookingEndLocal?.slice(11, 16) ?? null;
}

function parsedBreak(row) {
  return row.actual?.breakMinutes ?? row.wholeShiftInputs?.breakMinutes ?? null;
}

function sourceNormalForm(scenario, upload, parsed) {
  const expectedShape = buildExpectedSourceModel(scenario, upload);
  if (parsed.rows.length !== upload.physicalRows.length) {
    fail('WEEKLY_SOURCE_REAL_WORLD_PARSED_ROW_COUNT', `${upload.key} parsed an unexpected row count.`);
  }
  const rows = parsed.rows.map((row, ordinal) => {
    const declared = upload.physicalRows[ordinal];
    const expectedRow = expectedShape.rows[ordinal];
    const sourceTotal = row.pricingEvidence?.sourceTotalPence ?? null;
    return {
      ordinal,
      fixtureRowId: expectedRow.fixtureRowId,
      sourceRowKey: declared.key,
      candidateKey: declared.candidateKey,
      contractKey: declared.contractKey ?? null,
      requestId: row.referenceNumber ?? row.requestId ?? row.lineId ?? row.bookingReference ?? null,
      workDate: row.date ?? row.workDate,
      actualStart: parsedStart(row),
      actualEnd: parsedEnd(row),
      actualBreakMinutes: parsedBreak(row),
      actualWorkedMinutes: parsedMinutes(row),
      sign: row.physicalSign ?? (row.rowKind === 'FULL_REVERSAL' ? 'FULL_NEGATIVE' : 'POSITIVE'),
      finalisation: row.sourcePosition === 'WORKED' || row.rowKind === 'FINALISED_WORKED'
        ? 'FINALISED'
        : declared.finalisation,
      statusText: declared.statusText ?? null,
      finalisedBy: row.finalisedBy ?? declared.finalisedBy ?? null,
      sourceExpensePence: row.sourceFixedExpense?.pence ?? null,
      nhspSignedInvoiceExVatPence: upload.profile === 'NHSP_FINAL_BACKING_V1' ? sourceTotal : null,
      pricingEvidence: upload.profile === 'NHSP_FINAL_BACKING_V1' ? {
        commissionPence: row.pricingEvidence?.commissionPence ?? null,
        totalCostPence: row.pricingEvidence?.totalCostPence ?? null,
      } : null,
    };
  });
  const model = {
    modelVersion: expectedShape.modelVersion,
    scenarioId: scenario.scenarioId,
    uploadKey: upload.key,
    profile: upload.profile,
    stage: upload.stage,
    scope: cloneJson(expectedShape.scope),
    mutations: cloneJson(upload.mutations || []),
    expectedAcceptance: (upload.mutations || []).length === 0,
    rows,
  };
  return deepFreeze({ ...model, modelDigest: canonicalDigest(model) });
}

function schedule(shifts) {
  return shifts.map((shift) => ({
    date: shift.workDate,
    start: shift.start,
    end: shift.end,
    break_minutes: shift.breakMinutes,
    break_start: shift.breakStart,
    break_end: shift.breakEnd,
    worked_minutes: shift.workedMinutes,
    reference: shift.reference,
    additional_units: shift.additionalUnits,
  }));
}

function rates(record) {
  const pay = record.rates.pay;
  const charge = record.rates.charge;
  const pounds = (pence) => Number(pence) / 100;
  return {
    paye_day: pounds(pay.day), paye_night: pounds(pay.night), paye_sat: pounds(pay.saturday),
    paye_sun: pounds(pay.sunday), paye_bh: pounds(pay.bankHoliday),
    charge_day: pounds(charge.day), charge_night: pounds(charge.night), charge_sat: pounds(charge.saturday),
    charge_sun: pounds(charge.sunday), charge_bh: pounds(charge.bankHoliday),
  };
}

function records(plan, kind) {
  return plan.stages.flatMap((stage) => stage.records).filter((record) => record.recordKind === kind);
}

function recordMap(plan, kind) {
  return new Map(records(plan, kind).map((record) => [record.key, record]));
}

function buildFoundationSql(scenario, plan, state) {
  const remap = (map, links = []) => new Map([...map].map(([key, record]) => [key, {
    ...record,
    id: uuid(record.id),
    ...Object.fromEntries(links.map((link) => [link, record[link] ? uuid(record[link]) : null])),
  }]));
  const users = remap(recordMap(plan, 'PERMITTED_TEST_USER'));
  const candidates = remap(recordMap(plan, 'CANDIDATE_PREREQUISITE'));
  const clients = remap(recordMap(plan, 'CLIENT_PREREQUISITE'));
  const contracts = remap(recordMap(plan, 'CONTRACT_AND_RATE_PREREQUISITE'), ['candidateId', 'clientId']);
  const weeks = remap(recordMap(plan, 'CONTRACT_WEEK_PREREQUISITE'), ['contractId']);
  const timesheets = remap(recordMap(plan, 'CANDIDATE_TIMESHEET_EVIDENCE_PREREQUISITE'), ['weekId']);
  const actor = users.get('office_1');
  state.actorId = actor.id;
  state.agencyId = uuid(records(plan, 'AGENCY_AND_GLOBAL_SETTINGS')[0].id);
  state.ids = { users, candidates, clients, contracts, weeks, timesheets };

  const statements = [
    `insert into public.settings_defaults(id,candidate_manager_email_templates_sha256,candidate_home_announcement_sha256) values(1,decode(repeat('01',32),'hex'),decode(repeat('02',32),'hex')) on conflict(id) do update set candidate_manager_email_templates_sha256=excluded.candidate_manager_email_templates_sha256`,
    ...[...users.values()].map((item) => `insert into public.tms_users(id,email,role,is_active,password_hash,payment_authoriser) values(${literal(item.id)}::uuid,${literal(`${item.key}@example.test`)},'admin',true,'not-a-login',true)`),
    ...[...clients.values()].map((item) => `insert into public.clients(id,name,ts_queries_email) values(${literal(item.id)}::uuid,${literal(item.name)},${literal(item.managerEmail)})`),
    ...scenario.foundation.clients.map((declared) => {
      const item = clients.get(declared.key);
      return `insert into public.client_settings(client_id,vat_rate_pct,effective_from,is_nhsp,autoprocess_hr,requires_hr,no_timesheet_required,pay_reference_required) values(${literal(item.id)}::uuid,20,'2026-01-01',${declared.settings.nhsp},${declared.settings.requiresHealthRoster},${declared.settings.requiresHealthRoster},${declared.settings.noTimesheetRequired},${declared.settings.referenceRequiredBeforePay})`;
    }),
    ...[...candidates.values()].map((item) => {
      const normalizedRosterName = item.displayName.toLowerCase().replace(/[^a-z0-9]+/g, '');
      return `insert into public.candidates(id,tms_ref,display_name,pay_method,active,nhsp_hr_name_aliases) values(${literal(item.id)}::uuid,${literal(item.tmsRef)},${literal(item.displayName)},${literal(item.payMethod)},${item.active},${json([normalizedRosterName])})`;
    }),
  ];
  for (const declared of scenario.foundation.contracts) {
    const item = contracts.get(declared.key);
    const authority = scenario.foundation.clients.find((client) => client.key === declared.clientKey);
    statements.push(`insert into public.contracts(id,candidate_id,client_id,role,band,start_date,end_date,pay_method_snapshot,rates_json,weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr,require_reference_to_pay,healthroster_import_auto_authorise_override) values(${literal(item.id)}::uuid,${literal(item.candidateId)}::uuid,${literal(item.clientId)}::uuid,${literal(item.role)},${literal(item.band)},${literal(item.startDate)}::date,${literal(item.endDate)}::date,${literal(item.payMethod)},${json(rates(declared))},'HEALTHROSTER',${authority.settings.selfBill},${authority.settings.noTimesheetRequired},${authority.settings.requiresHealthRoster},${authority.settings.autoAuthorise},${authority.settings.referenceRequiredBeforePay},${authority.settings.autoAuthorise})`);
    if (authority.settings.requiresHealthRoster) {
      statements.push(`insert into public.assignment_band_mappings(system_type,incoming_code,band_match_pattern,active,candidate_id,client_id,target_contract_id) values('HR_WEEKLY',${literal(declared.band)},${literal(declared.band.toLowerCase())},true,${literal(item.candidateId)}::uuid,${literal(item.clientId)}::uuid,${literal(item.id)}::uuid)`);
    }
  }
  for (const declared of scenario.foundation.timesheets) {
    const item = timesheets.get(declared.key);
    const week = weeks.get(declared.weekKey);
    const contract = [...contracts.values()].find((candidate) => candidate.id === week.contractId);
    const client = scenario.foundation.clients.find((entry) => clients.get(entry.key).id === contract.clientId);
    const submissionMode = client.sourceAuthority === 'SOURCE' ? 'MANUAL' : 'ELECTRONIC';
    const rootSchedule = schedule(declared.shifts);
    const totalMinutes = declared.shifts.reduce((sum, shift) => sum + shift.workedMinutes, 0);
    const bookingId = `WS-${scenario.scenarioId}-${declared.key}`;
    state.bookingIds ??= new Map();
    state.bookingIds.set(declared.key, bookingId);
    statements.push(`insert into public.timesheets(timesheet_id,booking_id,version,is_current,status,sheet_scope,submission_mode,line_type,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,shift_label_norm,week_ending_date,contract_id,actual_schedule_json,qr_payload_json,r2_nurse_key,r2_auth_key,img_sha256_nurse,img_sha256_auth,is_adjustment) values(${literal(item.id)}::uuid,${literal(bookingId)},1,true,'RECEIVED','WEEKLY',${literal(submissionMode)},'HOURS',${literal(declared.key)},${literal(client.name)},'scenario-ward',${literal(contract.role || 'Nurse')},'weekly-0',${literal(week.weekEndingDate)}::date,${literal(contract.id)}::uuid,${json(rootSchedule)},'{}'::jsonb,${literal(`test/${scenario.scenarioId}/${declared.key}/candidate.png`)},${literal(`test/${scenario.scenarioId}/${declared.key}/authority.png`)},${literal(createHash('sha256').update(`${scenario.scenarioId}:${declared.key}:candidate`).digest('hex'))},${literal(createHash('sha256').update(`${scenario.scenarioId}:${declared.key}:authority`).digest('hex'))},false)`);
    statements.push(`insert into public.contract_weeks(id,contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot,timesheet_id,day_entries_json,totals_json,is_adjustment) values(${literal(week.id)}::uuid,${literal(week.contractId)}::uuid,${literal(week.weekEndingDate)}::date,${week.additionalSequence},'SUBMITTED',${literal(submissionMode)},${literal(item.id)}::uuid,${json(rootSchedule)},${json({ total_minutes: totalMinutes })},false)`);
    statements.push(`insert into public.timesheets_financials(id,timesheet_id,timesheet_version,is_current,candidate_id,client_id,role,band,pay_method,processing_status,total_hours,total_pay_ex_vat,total_charge_ex_vat,actual_schedule_json,expenses_pay_ex_vat,expenses_charge_ex_vat) values(${literal(uuid(`${scenario.scenarioId}:financial:${declared.key}`))}::uuid,${literal(item.id)}::uuid,1,true,${literal(contract.candidateId)}::uuid,${literal(contract.clientId)}::uuid,${literal(contract.role)},${literal(contract.band)},${literal(contract.payMethod)},'PENDING_AUTH',${totalMinutes / 60},0,0,${json(rootSchedule)},${Number(declared.expenses.amountPence) / 100},${Number(declared.expenses.amountPence) / 100})`);
  }

  const clientByKey = new Map(scenario.foundation.clients.map((item) => [item.key, item]));
  const uploadGroups = new Map();
  for (const upload of scenario.sourceUploads) {
    const family = upload.profile.startsWith('NHSP_') ? 'NHSP' : 'ROSTER';
    const groupKey = family === 'NHSP' ? 'NHSP' : upload.clientKey;
    if (!uploadGroups.has(groupKey)) {
      const groupId = uuid(`${scenario.scenarioId}:group:${groupKey}`);
      uploadGroups.set(groupKey, groupId);
      const client = clientByKey.get(upload.clientKey);
      const groupCode = `RW_${createHash('sha256').update(`${scenario.scenarioId}:${groupKey}`).digest('hex').slice(0, 20).toUpperCase()}`;
      statements.push(`insert into public.weekly_source_groups(id,environment,agency_id,code,display_name,source_family,timezone,cutoff_weekday,cutoff_local_time,nhsp_report_heading_name,active,updated_by_user_id) values(${literal(groupId)}::uuid,'TEST',${literal(state.agencyId)}::uuid,${literal(groupCode)},${literal(`${scenario.title} ${groupKey}`)},${literal(family)},'Europe/London',${client.settings.cutoffWeekday},${literal(`${client.settings.cutoffLocalTime}:00`)}::time,${family === 'NHSP' ? literal('Scenario Agency') : 'null'},true,${literal(actor.id)}::uuid)`);
      statements.push(`insert into public.weekly_source_group_clients(id,source_group_id,client_id,valid_from,created_by_user_id) values(${literal(uuid(`${scenario.scenarioId}:membership:${groupKey}`))}::uuid,${literal(groupId)}::uuid,${literal(clients.get(upload.clientKey).id)}::uuid,'2026-01-01',${literal(actor.id)}::uuid)`);
      statements.push(`insert into public.weekly_source_client_policies(id,source_group_id,client_id,effective_from,authority_mode,document_mode,self_bill_enabled,self_bill_correction_presentation,source_fixed_expenses_enabled,source_expense_vat_enabled,weekly_rate_classification_method,duration_break_tie_rule,candidate_queries_enabled,manager_queries_enabled,manager_query_recipient,created_by_user_id) values(${literal(uuid(`${scenario.scenarioId}:policy:${groupKey}`))}::uuid,${literal(groupId)}::uuid,${literal(clients.get(upload.clientKey).id)}::uuid,'2026-01-01',${literal(client.sourceAuthority === 'SIGNED_TIMESHEET' ? 'TIMESHEET_AUTHORITY' : 'SOURCE_AUTHORITY')},${literal(client.sourceAuthority === 'SIGNED_TIMESHEET' ? 'INVOICE_EVIDENCE_REQUIRED' : 'CHECK_ONLY')},${client.settings.selfBill},'FULL_REVERSAL_REPLACEMENT',${client.settings.sourceSuppliedExpenses},${client.settings.sourceExpenseVatChargeable},${literal(client.settings.calculationMode === 'WHOLE_SHIFT' ? 'WHOLE_SHIFT_START_DAY' : 'SPLIT_RATE_WINDOWS')},${client.settings.calculationMode === 'WHOLE_SHIFT' ? 'null' : literal('EARLIEST_LONGEST_PORTION')},${client.settings.candidateQueriesEnabled},${client.settings.managerQueriesEnabled},${literal(client.managerEmail)},${literal(actor.id)}::uuid)`);
    }
    const groupId = uploadGroups.get(groupKey);
    const cycleLocalDate = new Date(upload.cycleUtc).toISOString().slice(0, 10);
    const cycleKey = `${groupKey}:${cycleLocalDate}`;
    const cycleId = uuid(`${scenario.scenarioId}:cycle:${cycleKey}`);
    const weekEnding = cycleLocalDate;
    if (!state.cycles?.has(cycleKey)) {
      state.cycles ??= new Map();
      state.cycles.set(cycleKey, cycleId);
      statements.push(`insert into public.weekly_source_cycles(id,source_group_id,finalisation_week_ending,cutoff_at_utc,state,version,projection_state) values(${literal(cycleId)}::uuid,${literal(groupId)}::uuid,${literal(weekEnding)}::date,${literal(upload.cycleUtc)}::timestamptz,'OPEN',0,'NONE')`);
    }
    let reportScopeId = null;
    if (family === 'NHSP' && upload.profile === 'NHSP_FINAL_BACKING_V1') {
      reportScopeId = uuid(`${scenario.scenarioId}:scope:${upload.key}`);
      statements.push(`insert into public.weekly_source_report_scopes(id,source_cycle_id,environment,agency_id,source_group_id,client_id,cutoff_at_utc,version,state,projection_state) values(${literal(reportScopeId)}::uuid,${literal(cycleId)}::uuid,'TEST',${literal(state.agencyId)}::uuid,${literal(groupId)}::uuid,${literal(clients.get(upload.clientKey).id)}::uuid,(select cutoff_at_utc from public.weekly_source_cycles where id=${literal(cycleId)}::uuid),0,'OPEN','NONE')`);
    }
    state.uploadScopes ??= new Map();
    state.uploadScopes.set(upload.key, { groupId, cycleId, reportScopeId, clientId: clients.get(upload.clientKey).id });
  }
  return `begin; set local request.jwt.claim.role='service_role'; ${statements.join(';\n')}; commit;`;
}

async function rpcAt(origin, name, args) {
  const response = await fetch(`${origin}/rest/v1/rpc/${name}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(args || {}),
  });
  const payload = await response.json();
  if (!response.ok) fail(payload?.message || payload?.code || 'WEEKLY_SOURCE_RPC_FAILED', `${name} failed.`, payload);
  return payload;
}

function sourceForAction(scenario, action) {
  const upload = scenario.sourceUploads.find((item) => item.key === action.uploadKey);
  if (!upload) fail('WEEKLY_SOURCE_REAL_WORLD_UPLOAD_UNKNOWN', `Action references unknown upload ${action.uploadKey}.`);
  return upload;
}

function resultOwner(owner, result) {
  return deepFreeze({ owner, seededOutcome: false, result: cloneJson(result ?? {}) });
}

function keyForRequest(scenario, requestId) {
  for (const upload of scenario.sourceUploads) {
    const row = upload.physicalRows.find((item) => item.requestId === requestId);
    if (row) return row.key;
  }
  return null;
}

function hhmm(value) {
  if (value === null || value === undefined) return null;
  const text = String(value);
  const time = text.includes('T') ? text.slice(text.indexOf('T') + 1) : text;
  return time.slice(0, 5);
}

function approvedHoursReadback(database, scenario, state) {
  return scenario.foundation.timesheets.map((declaredTimesheet) => {
    const timesheet = state.ids.timesheets.get(declaredTimesheet.key);
    const week = scenario.foundation.weeks.find((item) => item.key === declaredTimesheet.weekKey);
    const contract = scenario.foundation.contracts.find((item) => item.key === week.contractKey);
    const client = scenario.foundation.clients.find((item) => item.key === contract.clientKey);
    const [readback] = queryJson(database, `
      with family as (
        select pg_catalog.unnest(private.weekly_source_invoice_family_timesheet_ids_v1(${literal(timesheet.id)}::uuid)) timesheet_id
      ), current_head as (
        select head.id from public.weekly_source_entitlement_heads head
        where head.state='COMMITTED_CURRENT' and head.root_timesheet_id in (select timesheet_id from family)
      ), latest_upload as (
        select row.upload_id
        from public.weekly_source_billing_movements movement
        join public.weekly_source_upload_rows row on row.id=coalesce(movement.nhsp_upload_row_id,nullif(movement.source_facts_json->>'upload_row_id','')::uuid)
        where movement.contract_id=${literal(state.ids.contracts.get(contract.key).id)}::uuid
        order by movement.created_at_utc desc,movement.id desc limit 1
      ), approved_source as (
        select distinct row.id,row.work_date,row.start_at_local,row.end_at_local,row.break_minutes,
          row.actual_net_minutes,row.external_source_key
        from current_head head
        join public.weekly_source_entitlement_head_components component on component.head_id=head.id
        join public.weekly_source_billing_movements movement on movement.id=component.movement_id
        join public.weekly_source_upload_rows row on row.id=coalesce(movement.nhsp_upload_row_id,nullif(movement.source_facts_json->>'upload_row_id','')::uuid)
        where movement.movement_role<>'REVERSAL' and row.actual_net_minutes>0
        union
        select distinct row.id,row.work_date,row.start_at_local,row.end_at_local,row.break_minutes,
          row.actual_net_minutes,row.external_source_key
        from public.weekly_source_upload_rows row
        join latest_upload latest on latest.upload_id=row.upload_id
        join public.weekly_source_billing_movements fallback_movement on coalesce(fallback_movement.nhsp_upload_row_id,nullif(fallback_movement.source_facts_json->>'upload_row_id','')::uuid)=row.id
        join public.weekly_source_row_resolutions resolution on resolution.upload_row_id=row.id
        where not exists(select 1 from current_head)
          and resolution.contract_id=${literal(state.ids.contracts.get(contract.key).id)}::uuid
          and resolution.mapping_state='RESOLVED' and fallback_movement.movement_role<>'REVERSAL' and row.actual_net_minutes>0
      )
      select t.actual_schedule_json,t.reference_number,t.day_references_json,
        (select pg_catalog.count(*) from current_head)::integer head_count,
        (select pg_catalog.count(*) from public.weekly_source_root_authorisations authorisation where authorisation.root_timesheet_id in (select timesheet_id from family) and authorisation.withdrawn_at_utc is null)::integer authorisation_count,
        coalesce((select pg_catalog.jsonb_agg(pg_catalog.to_jsonb(source_row) order by source_row.work_date,source_row.start_at_local,source_row.id) from approved_source source_row),'[]'::jsonb) source_schedule,
        coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('work_date',item.work_date,'reference_number',item.reference_number) order by item.work_date,item.item_ordinal)
          from public.weekly_timesheet_reference_apply_operations operation
          join public.weekly_timesheet_reference_apply_items item on item.operation_id=operation.id
          where operation.timesheet_id in (select timesheet_id from family) and operation.state='APPLIED' and item.applied_at_utc is not null),'[]'::jsonb) applied_references,
        coalesce((select pg_catalog.sum(coalesce(component.hours_day,0)+coalesce(component.hours_night,0)+coalesce(component.hours_sat,0)+coalesce(component.hours_sun,0)+coalesce(component.hours_bh,0)) from current_head head join public.weekly_source_entitlement_head_components component on component.head_id=head.id where component.exclude_from_pay is not true),0) approved_total_hours
      from public.timesheets t where t.timesheet_id=${literal(timesheet.id)}::uuid`);
    const headCount = Number(readback?.head_count);
    if (!readback || (headCount !== 1 && !(state.handover2BoundaryEmulated === true && headCount === 0 && Number(readback.authorisation_count) === 1))) {
      fail('WEEKLY_SOURCE_REAL_WORLD_APPROVED_HEAD_INVALID', `${declaredTimesheet.key} had neither one committed entitlement head nor the exact pending HANDOVER 2 boundary.`, { readback, authorisations: state.authorisations });
    }
    const sourceSchedule = Array.isArray(readback.source_schedule) ? readback.source_schedule : [];
    const actualSchedule = Array.isArray(readback.actual_schedule_json) ? readback.actual_schedule_json : [];
    const chosen = client.sourceAuthority === 'SOURCE' && sourceSchedule.length > 0 ? sourceSchedule : actualSchedule;
    const appliedReferences = Array.isArray(readback.applied_references) ? readback.applied_references : [];
    if (client.sourceAuthority === 'SIGNED_TIMESHEET' && appliedReferences.length !== chosen.length) {
      const importIds = [...state.uploaded.values()]
        .flatMap((value) => value.modeA?.dispatched?.clients ?? [])
        .map((value) => value.import_id)
        .filter(Boolean);
      const decisionDiagnostic = importIds.length === 0 ? [] : queryJson(database, `
        select action_kind,action_category,selectable,default_selected,selected,blocking,
          summary_json->>'reason_code' reason_code,
          summary_json->>'role' source_role,
          summary_json->'attachment_evidence' attachment_evidence
        from public.import_review_decisions
        where import_id=any(array[${importIds.map((id) => `${literal(id)}::uuid`).join(',')}]) and is_current
        order by action_id`);
      const phase2Diagnostic = importIds.flatMap((id) => queryJson(database, `select incoming_code,action,reason,contract_id from public.weekly_import_phase2(${literal(id)}::uuid,'HR_WEEKLY')`));
      fail('WEEKLY_SOURCE_REAL_WORLD_REFERENCE_APPLY_INCOMPLETE', `${declaredTimesheet.key} did not persist one applied reference for every signed Timesheet shift.`, { appliedReferences, chosen, modeADiagnostic: JSON.stringify([...state.uploaded.entries()].map(([key, value]) => ({ key, modeA: value.modeA }))), decisionDiagnostic, phase2Diagnostic });
    }
    const shifts = chosen.map((row) => {
      const workDate = row.work_date ?? row.date;
      const declared = declaredTimesheet.shifts.find((shift) => shift.workDate === workDate);
      return {
        key: declared?.key ?? `approved-${workDate}`,
        workDate,
        start: hhmm(row.start_at_local ?? row.start),
        end: hhmm(row.end_at_local ?? row.end),
        breakMinutes: Number(row.break_minutes ?? 0),
        breakStart: hhmm(row.break_start),
        breakEnd: hhmm(row.break_end),
        workedMinutes: Number(row.actual_net_minutes ?? row.worked_minutes ?? 0),
        reference: row.external_source_key ?? row.reference
          ?? appliedReferences.find((item) => item.work_date === workDate)?.reference_number
          ?? readback.reference_number ?? null,
        additionalUnits: cloneJson(row.additional_units ?? []),
      };
    });
    const scheduleHours = shifts.reduce((sum, shift) => sum + shift.workedMinutes, 0) / 60;
    if (headCount === 1 && Math.abs(scheduleHours - Number(readback.approved_total_hours)) > 0.000001) {
      fail('WEEKLY_SOURCE_REAL_WORLD_APPROVED_SCHEDULE_DIVERGED', `${declaredTimesheet.key} schedule did not equal its committed entitlement.`, {
        scheduleHours,
        approvedTotalHours: readback.approved_total_hours,
      });
    }
    return { candidateKey: contract.candidateKey, contractKey: contract.key, weekKey: week.key, shifts };
  });
}

function scenarioAssertionReadback(database, scenario, state, sourceMovements, invoiceLines, approvedHours) {
  const timesheetIds = [...state.ids.timesheets.values()].map((item) => item.id);
  const adjustmentTimesheets = Number(executePsql(database, `select count(*) from public.timesheets where timesheet_id=any(array[${timesheetIds.map((id) => `${literal(id)}::uuid`).join(',')}]) and is_adjustment`));
  const entitlementHeads = Number(executePsql(database, `select count(*) from public.weekly_source_entitlement_heads where root_timesheet_id=any(array[${timesheetIds.map((id) => `${literal(id)}::uuid`).join(',')}])`));
  if (scenario.scenarioId === 'WS-REAL-WORLD-NHSP-001') {
    const submittedMinutes = scenario.foundation.timesheets[0].shifts.reduce((sum, shift) => sum + shift.workedMinutes, 0);
    const approvedMinutes = approvedHours[0]?.shifts.reduce((sum, shift) => sum + shift.workedMinutes, 0);
    const secondInvoice = invoiceLines.filter((line) => line.cycleKey === 'final_2');
    const secondRoles = sourceMovements.filter((movement) => ['source_reversal', 'source_corrected'].includes(movement.sourceRowKey));
    if (state.readProjection.length !== 1 || submittedMinutes !== 450 || approvedMinutes !== 450
        || !sourceMovements.some((movement) => movement.sourceRowKey === 'source_original' && movement.workedMinutes === 390)) {
      fail('WEEKLY_SOURCE_REAL_WORLD_VISIBILITY_NOT_PROVED', 'NHSP submitted, source and approved-hour evidence did not remain independently readable.');
    }
    if (secondInvoice.length !== 2 || secondRoles.length !== 2
        || !secondRoles.some((movement) => movement.kind === 'PHYSICAL_FULL_NEGATIVE' && movement.invoiceExVatPence.startsWith('-'))
        || !secondRoles.some((movement) => movement.kind === 'PHYSICAL_POSITIVE' && !movement.invoiceExVatPence.startsWith('-'))) {
      fail('WEEKLY_SOURCE_REAL_WORLD_SIGNED_LINES_NOT_PROVED', 'The second NHSP self bill did not preserve its physical negative and positive rows separately.');
    }
    if (adjustmentTimesheets !== 0 || entitlementHeads > 1) {
      fail('WEEKLY_SOURCE_REAL_WORLD_DUPLICATE_PAY_ARTIFACT', 'The corrected NHSP source created a duplicate local Candidate-pay artifact.', { adjustmentTimesheets, entitlementHeads });
    }
    return {
      visibleAssertions: [
        'Submitted hours, source hours and approved hours remain separately visible',
        'The second self bill retains separate negative and positive lines',
        'The corrected source creates no duplicate Candidate payment',
      ],
      forbiddenAssertions: [
        'Candidate pay is calculated from invoice movement totals',
        'The negative and positive invoice movements are netted into one line',
        'A legacy reversal Timesheet pair is created',
      ],
      evidence: { submittedMinutes, approvedMinutes, secondInvoiceLineCount: secondInvoice.length, adjustmentTimesheets, entitlementHeads },
    };
  }
  if (scenario.scenarioId === 'WS-REAL-WORLD-ROSTER-001') {
    const sourceHours = approvedHours.find((item) => item.contractKey === 'contract_source')?.shifts?.[0];
    const signedHours = approvedHours.find((item) => item.contractKey === 'contract_signed')?.shifts?.[0];
    const signedTimesheet = state.ids.timesheets.get('timesheet_signed');
    const fixedContract = state.ids.contracts.get('contract_fixed');
    const [fixedEvidence] = queryJson(database, `
      select
        count(*) filter(where movement.source_line_kind='SOURCE_FIXED_EXPENSE')::integer expense_lines,
        count(distinct movement.invoice_timesheet_id)::integer invoice_timesheet_count,
        count(*) filter(where timesheet.line_type='EXPENSES')::integer separate_expense_timesheets
      from public.weekly_source_billing_movements movement
      join public.timesheets timesheet on timesheet.timesheet_id=movement.invoice_timesheet_id
      where movement.contract_id=${literal(fixedContract.id)}::uuid`);
    const [signedEvidence] = queryJson(database, `
      select
        count(*) filter(where operation.state='APPLIED' and item.applied_at_utc is not null)::integer applied_references,
        max(item.reference_number) reference_number,
        max(financial.expenses_pay_ex_vat)::numeric expenses_pay_ex_vat
      from public.weekly_timesheet_reference_apply_operations operation
      join public.weekly_timesheet_reference_apply_items item on item.operation_id=operation.id
      join public.timesheets_financials financial on financial.timesheet_id=operation.timesheet_id and financial.is_current
      where operation.timesheet_id=${literal(signedTimesheet.id)}::uuid`);
    if (!sourceHours || sourceHours.start !== '20:00' || sourceHours.end !== '08:00'
        || sourceHours.breakMinutes !== 60 || sourceHours.workedMinutes !== 660) {
      fail('WEEKLY_SOURCE_REAL_WORLD_ACTUAL_HOURS_NOT_PROVED', 'HealthRoster Actual start, end and break were not the approved source-authority hours.', { sourceHours });
    }
    if (!signedHours || signedHours.reference !== 'HR-B-001'
        || Number(signedEvidence?.applied_references) !== 1 || signedEvidence?.reference_number !== 'HR-B-001') {
      fail('WEEKLY_SOURCE_REAL_WORLD_REFERENCE_NOT_PROVED', 'The signed Timesheet did not retain its own hours and receive the exact source reference.', { signedHours, signedEvidence });
    }
    if (Number(signedEvidence?.expenses_pay_ex_vat) !== 25) {
      fail('WEEKLY_SOURCE_REAL_WORLD_SIGNED_EXPENSE_NOT_PROTECTED', 'The ordinary signed-Timesheet expense value was not preserved.', { signedEvidence });
    }
    if (Number(fixedEvidence?.expense_lines) !== 1 || Number(fixedEvidence?.invoice_timesheet_count) !== 1
        || Number(fixedEvidence?.separate_expense_timesheets) !== 0 || adjustmentTimesheets !== 0) {
      fail('WEEKLY_SOURCE_REAL_WORLD_FIXED_EXPENSE_LINEAGE_INVALID', 'The source-fixed expense did not stay on the same source Timesheet.', { fixedEvidence, adjustmentTimesheets });
    }
    return {
      visibleAssertions: [
        'Source-authority hours use finalised Actual start, end and break',
        'Signed Timesheet hours remain authoritative and receive the matching reference',
        'The source-fixed expense stays on the source Timesheet without receipt upload',
      ],
      forbiddenAssertions: [
        'Contract hours replace Actual hours',
        'The signed Timesheet expense journey is removed',
        'A source-fixed expense creates a separate expense Timesheet',
      ],
      evidence: { sourceHours, signedHours, signedEvidence, fixedEvidence, adjustmentTimesheets },
    };
  }
  fail('WEEKLY_SOURCE_REAL_WORLD_ASSERTION_SCENARIO_UNKNOWN', `No database assertion owner exists for ${scenario.scenarioId}.`);
}

export async function createRealWorldScenarioDependencies({ mode, scenario, plan, database: parentDatabase }) {
  databasePort(parentDatabase.connectionUrl);
  const childName = `ws_rw_${createHash('sha256').update(`${mode}:${scenario.scenarioId}`).digest('hex').slice(0, 20)}`;
  const adminDatabase = {
    ...parentDatabase,
    connectionUrl: withDatabase(parentDatabase.connectionUrl, 'postgres'),
    database: 'postgres',
  };
  executePsql(adminDatabase, `drop database if exists ${childName} with (force);`, { tuples: false });
  executePsql(adminDatabase, `create database ${childName} with template ${parentDatabase.database} owner postgres;`, { tuples: false });
  let database = {
    ...parentDatabase,
    connectionUrl: withDatabase(parentDatabase.connectionUrl, childName),
    database: childName,
  };
  const state = {
    mode,
    scenario,
    plan,
    database,
    uploaded: new Map(),
    actionOwners: [],
    readProjection: [],
    sourceBytes: new Map(),
  };
  const postgrest = createLocalPostgrest({
    database: database.database,
    psqlBin: database.psqlBin,
    databasePort: databasePort(database.connectionUrl),
  });
  const origin = await postgrest.listen();
  const rpc = (name, args) => rpcAt(origin, name, args);
  const uploadOwner = createWeeklySourceUploadPublicationOwner({ rpc });

  return {
    allowedLoopbackOrigins: [origin],
    sourceDefinitions: { GENERIC_WEEKLY_COMPLETE_V1: FIXED_EXPENSE_PROFILE },
    nhspReportHeadingName: 'Scenario Agency',
    async parseSource({ upload, artifact }) {
      state.sourceBytes.set(upload.key, Buffer.from(artifact.bytes));
      const parsed = await parseWeeklySourceFile(artifact.bytes, parserOptions(upload));
      if (parsed.ok !== true) fail('WEEKLY_SOURCE_REAL_WORLD_PARSE_FAILED', `${upload.key} was rejected by the production parser.`);
      return {
        parserProfile: parsed.profileId,
        parserVersion: parsed.parserVersion || parsed.profileVersion,
        normalForm: sourceNormalForm(scenario, upload, parsed),
      };
    },
    foundation: {
      async applyPrerequisites({ plan: foundationPlan }) {
        executePsql(database, buildFoundationSql(scenario, foundationPlan, state), { tuples: false });
      },
      async auditPrerequisites({ plan: foundationPlan }) {
        const counts = JSON.parse(executePsql(database, `select pg_catalog.jsonb_build_object('users',(select count(*) from public.tms_users where id=${literal(state.actorId)}::uuid),'clients',(select count(*) from public.clients where id=any(array[${[...state.ids.clients.values()].map((item) => `${literal(item.id)}::uuid`).join(',')}])),'candidates',(select count(*) from public.candidates where id=any(array[${[...state.ids.candidates.values()].map((item) => `${literal(item.id)}::uuid`).join(',')}])),'timesheets',(select count(*) from public.timesheets where timesheet_id=any(array[${[...state.ids.timesheets.values()].map((item) => `${literal(item.id)}::uuid`).join(',')}])) )::text;`));
        if (Number(counts.users) !== 1 || Number(counts.clients) !== state.ids.clients.size
            || Number(counts.candidates) !== state.ids.candidates.size || Number(counts.timesheets) !== state.ids.timesheets.size) {
          fail('WEEKLY_SOURCE_REAL_WORLD_FOUNDATION_READBACK_FAILED', 'Foundation prerequisites did not read back exactly.');
        }
        return {
          passed: true,
          scenarioId: scenario.scenarioId,
          planDigest: foundationPlan.planDigest,
          preconditionDigest: canonicalDigest(counts),
        };
      },
    },
    async executeProductAction({ action, generatedSources, effects }) {
      state.effects = effects;
      const actor = { id: state.actorId };
      if (action.kind === 'UPLOAD_SOURCE') {
        const upload = sourceForAction(scenario, action);
        const generated = generatedSources.find((item) => item.uploadKey === upload.key);
        const scope = state.uploadScopes.get(upload.key);
        const body = {
          source_group_id: scope.groupId,
          source_cycle_id: scope.cycleId,
          report_scope_id: scope.reportScopeId,
          client_id: scope.clientId,
          profile_id: parseProfile(upload),
          original_filename: generated.fileName,
          coverage: { start_local_date: upload.coverageStart, end_local_date: upload.coverageEnd },
        };
        const artifactBytes = state.sourceBytes.get(upload.key);
        if (!artifactBytes) fail('WEEKLY_SOURCE_REAL_WORLD_SOURCE_BYTES_MISSING', `${upload.key} bytes were not retained by the parser adapter.`);
        const result = await uploadOwner.acceptUpload({ body, bytes: artifactBytes, actor, parseWeeklySourceFile });
        const client = scenario.foundation.clients.find((item) => item.key === upload.clientKey);
        let modeA = null;
        if (client?.sourceAuthority === 'SIGNED_TIMESHEET') {
          const dispatched = await rpc('weekly_source_mode_a_dispatch_atomic_v1', { p_request: {
            actor_user_id: state.actorId,
            publication_id: result.publication_id,
          } });
          if (dispatched?.dispatched === true) {
            const applied = await rpc('weekly_source_mode_a_reference_apply_atomic_v1', { p_request: {
              actor_user_id: state.actorId,
              publication_id: result.publication_id,
            } });
            modeA = { dispatched, applied };
          }
        }
        state.uploaded.set(upload.key, { ...result, body, upload, modeA });
        return resultOwner('broker/src/weekly-source/index.js#parseWeeklySourceFile', { ...result, mode_a: modeA });
      }
      if (action.kind === 'CONFIRM_CURRENT_UPLOAD') {
        const uploaded = state.uploaded.get(action.uploadKey);
        const [proof] = queryJson(database, `select encode(comparison_manifest_hash,'hex') comparison_hash,encode(issue_set_hash,'hex') issue_hash from public.weekly_source_projection_publications where id=${literal(uploaded.publication_id)}::uuid`);
        const result = await rpc('weekly_source_projection_publish_atomic_v1', { p_request: {
          actor_user_id: state.actorId,
          publication_id: uploaded.publication_id,
          expected_comparison_manifest_hash: proof.comparison_hash,
          expected_issue_set_hash: proof.issue_hash,
        } });
        return resultOwner('public.weekly_source_projection_publish_atomic_v1(jsonb)', result);
      }
      if (action.kind === 'PROTECT_HOURS') {
        const timesheet = state.ids.timesheets.get(action.timesheetKey);
        const declared = scenario.foundation.timesheets.find((item) => item.key === action.timesheetKey);
        const week = state.ids.weeks.get(declared.weekKey);
        const contract = [...state.ids.contracts.values()].find((item) => item.id === week.contractId);
        const client = [...state.ids.clients.values()].find((item) => item.id === contract.clientId);
        const shift = declared.shifts.find((item) => item.key === action.shiftKey);
        const startAt = `${shift.workDate}T${shift.start}:00`;
        const endDate = shift.end > shift.start
          ? shift.workDate
          : new Date(`${shift.workDate}T12:00:00Z`).toISOString().slice(0, 10).replace(/(\d{4})-(\d{2})-(\d{2})/, (_all, y, m, d) => {
            const next = new Date(Date.UTC(Number(y), Number(m) - 1, Number(d) + 1));
            return next.toISOString().slice(0, 10);
          });
        const result = await rpc('weekly_exceptional_pay_prepare_family_v1', { p_request: {
          actor_user_id: state.actorId,
          source_cycle_id: state.uploadScopes.get('final_1')?.cycleId ?? [...state.cycles.values()][0],
          candidate_id: contract.candidateId, client_id: client.id, contract_id: contract.id,
          week_ending_date: week.weekEndingDate, work_date: shift.workDate,
          start_at_local: startAt, end_at_local: `${endDate}T${shift.end}:00`, break_minutes: shift.breakMinutes,
          evidence_timesheet_id: timesheet.id, reason: action.reason,
          idempotency_key: `${scenario.scenarioId}:protect:${action.timesheetKey}:${action.shiftKey}`,
        } });
        state.protected = result;
        return resultOwner('public.weekly_exceptional_pay_prepare_family_v1(jsonb)', result);
      }
      if (action.kind === 'FINALISE') {
        const uploaded = state.uploaded.get(action.uploadKey);
        const rows = queryJson(database, `select encode(u.row_manifest_hash,'hex') row_hash,encode(p.comparison_manifest_hash,'hex') comparison_hash,encode(p.issue_set_hash,'hex') issue_hash from public.weekly_source_uploads u join public.weekly_source_projection_publications p on p.id=${literal(uploaded.publication_id)}::uuid where u.id=${literal(uploaded.upload_id)}::uuid`);
        const proof = rows[0];
        const scope = state.uploadScopes.get(action.uploadKey);
        const result = await rpc('weekly_source_finalise_atomic_v1', { p_request: {
          actor_user_id: state.actorId, source_cycle_id: scope.cycleId,
          authority_scope_kind: scope.reportScopeId ? 'NHSP_REPORT_SCOPE' : 'CYCLE',
          report_scope_id: scope.reportScopeId, upload_id: uploaded.upload_id,
          projection_publication_id: uploaded.publication_id,
          expected_authority_scope_version: uploaded.authority_scope_version,
          expected_row_manifest_hash: proof.row_hash,
          expected_comparison_manifest_hash: proof.comparison_hash,
          expected_issue_set_hash: proof.issue_hash,
        } });
        state.finalisations ??= [];
        state.finalisations.push(result);
        return resultOwner('public.weekly_source_finalise_atomic_v1(jsonb)', result);
      }
      if (action.kind === 'AUTHORISE') {
        const evidenceTimesheet = state.ids.timesheets.get(action.timesheetKey);
        const declared = scenario.foundation.timesheets.find((item) => item.key === action.timesheetKey);
        const week = state.ids.weeks.get(declared.weekKey);
        const lineageRoot = executePsql(database, `select coalesce((select lineage.timesheet_id::text from public.weekly_source_row_timesheet_lineages lineage where lineage.contract_id=${literal(week.contractId)}::uuid order by lineage.created_at_utc desc limit 1),'');`);
        const timesheetId = lineageRoot || evidenceTimesheet.id;
        const signature = executePsql(database, `select coalesce(nullif(btrim(coalesce(signature->>'backend_row_signature',signature->>'row_signature','')),''),'') from public.timesheet_lifecycle_guard_signature_v1(${literal(timesheetId)}::uuid,(select id from public.contract_weeks where timesheet_id=${literal(timesheetId)}::uuid),false) signature;`);
        const result = await rpc('weekly_source_first_authorise_v1', {
          p_timesheet_id: timesheetId, p_expected_timesheet_id: timesheetId,
          p_expected_row_signature: signature, p_actor_user_id: state.actorId,
        });
        let finalResult = result;
        if (result?.code === 'WEEKLY_SOURCE_CANDIDATE_BUSY') {
          if (parentDatabase.componentBoundary !== true) {
            fail('WEEKLY_SOURCE_HANDOVER2_BOUNDARY_PENDING', 'The full journey requires the separately owned Workbench implementation.', { result });
          }
          executePsql(database, `update public.banking_pay_workbench_jobs set status='SUCCEEDED',completed_at_utc=pg_catalog.clock_timestamp() where public._pay_workbench_candidate_serial_candidate_id(candidate_id,payload_json)=${literal(result.candidate_id)}::uuid and status in ('QUEUED','RUNNING');`);
          executePsql(database, `update public.banking_pay_workbench_candidate_delta_projection_runs set status='COMPLETED',completed_at_utc=pg_catalog.clock_timestamp(),updated_at_utc=pg_catalog.clock_timestamp() where candidate_id=${literal(result.candidate_id)}::uuid and status in ('RUNNING','PROCESSING','IN_PROGRESS');`);
          state.handover2BoundaryEmulated = true;
          finalResult = await rpc('weekly_source_first_authorise_v1', {
            p_timesheet_id: timesheetId, p_expected_timesheet_id: timesheetId,
            p_expected_row_signature: signature, p_actor_user_id: state.actorId,
          });
        }
        state.authorisations ??= [];
        state.authorisations.push(finalResult);
        const bound = Number(executePsql(database, `select count(*) from public.weekly_source_root_authorisations where root_timesheet_id=${literal(timesheetId)}::uuid and withdrawn_at_utc is null;`));
        if (bound !== 1) {
          const serialState = queryJson(database, `select public._pay_workbench_candidate_serial_active_state(null,${literal(result.candidate_id)}::uuid,'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION','{}'::jsonb,null) state`);
          fail('WEEKLY_SOURCE_REAL_WORLD_AUTHORISATION_NOT_BOUND', 'The product authorisation owner did not bind the source root.', { result: finalResult, serialState });
        }
        return resultOwner('public.weekly_source_first_authorise_v1(uuid,uuid,text,uuid)', finalResult);
      }
      if (action.kind === 'CREATE_INVOICE_BATCH') {
        const query = {
          mode: 'PAGE', filters: {}, sort: {},
          selection: {
            contract_version: 'INVOICE_BATCH_SELECTION_V2', mode: 'IMPLICIT_ALL',
            default_selected: true, rules: [],
          },
        };
        const page = await loadWeeklySourceInvoiceBatchCandidates({ rpc }, query, null, { mode: 'PAGE' });
        let admission = null;
        if (page.rows.some((row) => row.selectable)) {
          const selectionKeys = page.rows.filter((row) => row.selectable).map((row) => row.selection_key);
          const revisions = Object.fromEntries(page.rows.filter((row) => row.selectable).map((row) => [row.selection_key, row.source_revision]));
          const preflight = await preflightWeeklySourceInvoiceBatch({ rpc }, { ...query, mode: 'CONFIRM' }, {
            contract_version: WEEKLY_SOURCE_INVOICE_BATCH_SELECTION_CONTRACT,
            snapshot_hash: page.snapshot_hash,
          }, { selectionKeys, expectedSourceRevisions: revisions });
          admission = await admitWeeklySourceInvoiceBatch({ rpc }, state.actorId, `${scenario.scenarioId}:invoice:${state.invoiceSequence = (state.invoiceSequence || 0) + 1}`, preflight);
        }
        state.invoicePages ??= [];
        state.invoicePages.push({ page, admission });
        return resultOwner('broker/src/weekly-source/invoice-batch-integration.mjs', { row_count: page.rows.length, admitted: Boolean(admission) });
      }
      if (action.kind === 'RECONCILE_PROTECTED_HOURS') {
        const result = await rpc('weekly_exceptional_pay_action_publication_status_v1', { p_request: {
          schema_version: 'WEEKLY_PROTECTED_ACTION_PUBLICATION_STATUS_V1',
          actor_user_id: state.actorId,
          family_id: state.protected?.family_id,
          orchestration_run_id: state.protected?.orchestration_run_id,
        } });
        state.reconciliation = result;
        return resultOwner('public.weekly_exceptional_pay_action_publication_status_v1(jsonb)', result);
      }
      if (action.kind === 'READ_PROJECTION') {
        const timesheet = state.ids.timesheets.get(action.timesheetKey);
        const presentation = await rpc('weekly_source_office_timesheet_presentation_v1', { p_request: {
          actor_user_id: state.actorId, timesheet_id: timesheet.id,
        } });
        const audit = await rpc('weekly_source_timesheet_audit_chronology_v1', { p_request: {
          timesheet_id: timesheet.id,
        } });
        const hours = await rpc('weekly_source_timesheet_hours_export_v1', { p_request: {
          timesheet_id: timesheet.id,
        } });
        state.readProjection.push({ presentation, audit, hours });
        return resultOwner('public.weekly_source_office_timesheet_presentation_v1(jsonb)', { read: true });
      }
      fail('WEEKLY_SOURCE_REAL_WORLD_ACTION_UNSUPPORTED', `Unsupported action ${action.kind}.`);
    },
    async collectObservedState() {
      const movements = queryJson(database, `select r.external_source_key request_id,m.movement_role,m.source_line_kind,m.source_facts_json,m.canonical_pay_vector_json,m.invoice_presentation_charge_pence,u.id upload_id,report.backing_report_number from public.weekly_source_billing_movements m left join public.weekly_source_upload_rows r on r.id=coalesce(m.nhsp_upload_row_id,nullif(m.source_facts_json->>'upload_row_id','')::uuid) left join public.weekly_source_uploads u on u.id=r.upload_id left join public.weekly_source_nhsp_backing_reports report on report.upload_id=u.id where m.actual_client_id=any(array[${[...state.ids.clients.values()].map((item) => `${literal(item.id)}::uuid`).join(',')}]) and m.source_line_kind<>'SOURCE_FIXED_EXPENSE' order by m.created_at_utc,m.id`);
      const sourceMovements = movements.map((row) => ({
        sourceRowKey: keyForRequest(scenario, row.request_id),
        kind: row.movement_role === 'REVERSAL' ? 'PHYSICAL_FULL_NEGATIVE'
          : (scenario.tags.includes('NHSP') ? 'PHYSICAL_POSITIVE' : 'ADD'),
        workedMinutes: Number(row.canonical_pay_vector_json?.paid_minutes || 0) * (row.movement_role === 'REVERSAL' ? -1 : 1),
        invoiceExVatPence: String(row.invoice_presentation_charge_pence),
      }));
      const invoiceLines = movements.map((row) => {
        const sourceRowKey = keyForRequest(scenario, row.request_id);
        const declared = scenario.sourceUploads.flatMap((upload) => upload.physicalRows.map((physical) => ({ upload, physical }))).find((item) => item.physical.key === sourceRowKey);
        const uploaded = [...state.uploaded.entries()].find(([, item]) => item.upload_id === row.upload_id);
        if (!sourceRowKey || !declared || !uploaded) {
          fail('WEEKLY_SOURCE_REAL_WORLD_MOVEMENT_IDENTITY_UNRESOLVED', 'A billing movement could not be traced to its generated source row and upload.', { row, sourceRowKey, uploaded: uploaded?.[0] ?? null });
        }
        return {
          sourceRowKey,
          clientKey: declared?.upload.clientKey,
          cycleKey: uploaded?.[0] ?? null,
          reportNumber: row.backing_report_number ?? null,
          exVatPence: String(row.invoice_presentation_charge_pence),
        };
      });
      const approvedHours = approvedHoursReadback(database, scenario, state);
      const communications = queryJson(database, `
        select case when intent.audience_kind='CANDIDATE' then 'CANDIDATE' else 'MANAGER' end audience,
          intent.tranche_kind kind,count(*)::integer count,
          case when intent.audience_kind='CANDIDATE' then candidate.id::text else coalesce(route.protected_recipient_address,'') end group_key
        from public.weekly_message_intents intent
        left join public.weekly_candidate_outreach_generations generation on generation.id=intent.candidate_generation_id
        left join public.candidates candidate on candidate.id=generation.candidate_id
        left join public.weekly_manager_recipient_routes route on route.id=intent.recipient_route_id
        where intent.source_cycle_id=any(array[${[...state.cycles.values()].map((id) => `${literal(id)}::uuid`).join(',')}])
        group by intent.audience_kind,intent.tranche_kind,candidate.id,route.protected_recipient_address
        order by 1,2,4`).map((row) => ({ audience: row.audience, kind: row.kind, count: Number(row.count), groupKey: row.group_key }));
      const authorisationCount = Number(executePsql(database, `select count(*) from public.weekly_source_root_authorisations where root_timesheet_id=any(array[${[...state.ids.timesheets.values()].map((item) => `${literal(item.id)}::uuid`).join(',')}]);`));
      const committedHeadCount = Number(executePsql(database, `select count(*) from public.weekly_source_entitlement_heads where state='COMMITTED_CURRENT' and root_timesheet_id=any(array[${[...state.ids.timesheets.values()].map((item) => `${literal(item.id)}::uuid`).join(',')}]);`));
      const assertionReadback = scenarioAssertionReadback(database, scenario, state, sourceMovements, invoiceLines, approvedHours);
      state.assertionReadback = assertionReadback;
      return {
        outcome: 'SUCCESS', sourceMovements, invoiceLines, approvedHours, communications,
        c1PublicationCategory: committedHeadCount > 0 ? 'COMPLETE_ENTITLEMENT' : (authorisationCount > 0 ? 'WAITING' : 'NONE'),
        visibleAssertions: cloneJson(assertionReadback.visibleAssertions),
        forbiddenAssertions: cloneJson(assertionReadback.forbiddenAssertions),
      };
    },
    async collectProjections() {
      return [
        { name: 'audit', rows: cloneJson(state.readProjection), assertionEvidence: cloneJson(state.assertionReadback?.evidence ?? {}), text: (state.assertionReadback?.visibleAssertions ?? []).join('\n') },
        { name: 'forbiddenOutcomes', rows: queryJson(database, `select count(*)::integer legacy_reversal_pairs from public.timesheets where booking_id like ${literal(`WS-${scenario.scenarioId}%`)} and is_adjustment`) },
      ];
    },
    async collectC1() {
      const count = Number(executePsql(database, `select count(*) from public.weekly_source_root_authorisations where root_timesheet_id=any(array[${[...state.ids.timesheets.values()].map((item) => `${literal(item.id)}::uuid`).join(',')}]);`));
      const committed = Number(executePsql(database, `select count(*) from public.weekly_source_entitlement_heads where state='COMMITTED_CURRENT' and root_timesheet_id=any(array[${[...state.ids.timesheets.values()].map((item) => `${literal(item.id)}::uuid`).join(',')}]);`));
      const componentBoundary = parentDatabase.componentBoundary === true;
      return {
        category: committed > 0 ? 'COMPLETE_ENTITLEMENT' : (count > 0 ? 'WAITING' : 'NONE'),
        requestDigest: canonicalDigest({ scenario: scenario.scenarioId, count, committed }),
        emulator: componentBoundary,
        evidenceScope: componentBoundary ? 'LOCAL_LIMB_ONLY' : 'FULL_RELEASE_PATH',
        bankingPayBoundary: componentBoundary
          ? 'HANDOVER2_BOUNDARY_EMULATED'
          : 'HANDOVER2_BOUNDARY_EXECUTED',
      };
    },
    async collectCoverage() {
      return {
        acceptanceIds: scenario.requirementIds,
        protectedIds: scenario.protectedIds,
        modelIds: ['REAL_WORLD_NEW_UPGRADE_DATABASE_JOURNEY'],
      };
    },
    async repositoryEvidence() {
      return [repositoryReadback()];
    },
    async databaseEvidence() {
      const componentBoundary = parentDatabase.componentBoundary === true;
      return {
        used: true,
        engine: 'PostgreSQL',
        version: executePsql(database, "select current_setting('server_version');"),
        mode,
        rowsReadBack: true,
        evidenceScope: componentBoundary ? 'LOCAL_LIMB_ONLY' : 'FULL_RELEASE_PATH',
        bankingPayBoundary: componentBoundary
          ? 'HANDOVER2_BOUNDARY_EMULATED'
          : 'HANDOVER2_BOUNDARY_EXECUTED',
        releaseEvidenceEligible: !componentBoundary,
      };
    },
    async cleanupScenario() {
      await postgrest.close();
      executePsql(adminDatabase, `drop database if exists ${childName} with (force);`, { tuples: false });
      const residue = Number(executePsql(adminDatabase, `select count(*) from pg_catalog.pg_database where datname=${literal(childName)};`));
      return { complete: residue === 0, databaseDropped: residue === 0 };
    },
  };
}
