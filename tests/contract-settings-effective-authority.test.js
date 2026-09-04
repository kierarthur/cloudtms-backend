import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';

const worker = readFileSync(new URL('../broker/src/index.js', import.meta.url), 'utf8');
const invoiceAsyncHttp = readFileSync(new URL('../broker/src/invoice-async-http.js', import.meta.url), 'utf8');
const wrangler = readFileSync(new URL('../wrangler.toml', import.meta.url), 'utf8');
const migration = readFileSync(new URL('../supabase/migrations/03092026_1640_contract_settings_authority_snapshot.sql', import.meta.url), 'utf8');
const missingSettingsCanvasRepair = readFileSync(new URL('../supabase/migrations/04092026_1515_client_settings_missing_canvas_v1.sql', import.meta.url), 'utf8');
const processedDailyOriginRepair = readFileSync(new URL('../supabase/migrations/04092026_1610_client_settings_processed_daily_origin_backdate_v1.sql', import.meta.url), 'utf8');
const resolver = readFileSync(new URL('../supabase/repeatable/03092026_1641_contract_settings_effective_authority_v1.sql', import.meta.url), 'utf8');
const invoiceCore = readFileSync(new URL('../supabase/repeatable/02092026_1834_candidate_expense_separation_delivery_v1.sql', import.meta.url), 'utf8');
const invoiceResolver = readFileSync(new URL('../supabase/repeatable/07082026_2225_candidate_app_qr_settings_invoice_replacements_v1.sql', import.meta.url), 'utf8');
const invoiceVat = readFileSync(new URL('../supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_private_invoice_generation_vat_policy_batch.sql', import.meta.url), 'utf8');
const timesheetSummary = readFileSync(new URL('../supabase/repeatable/19122025_add_ready_to_pay_to_timesheets_summary_views.sql', import.meta.url), 'utf8');
const invoicePrecheck = readFileSync(new URL('../supabase/repeatable/08012026_v_ts_invoice_precheck.sql', import.meta.url), 'utf8');
const invoiceCorrection = readFileSync(new URL('../supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_private_invoice_correction_validate_batch.sql', import.meta.url), 'utf8');
const invoicePresentation = readFileSync(new URL('../supabase/repeatable/25072026_0002_private_invoice_presentation_snapshot_batch.sql', import.meta.url), 'utf8');
const invoiceApplyEdits = readFileSync(new URL('../supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_invoice_apply_edits.sql', import.meta.url), 'utf8');
const invoiceApplyEditsAcl = readFileSync(new URL('../supabase/repeatable/03092026_1644_invoice_apply_edits_frozen_authority_acl_v1.sql', import.meta.url), 'utf8');
const invoiceGenerationFinal = readFileSync(new URL('../supabase/repeatable/03092026_1645_invoice_generation_frozen_settings_authority_v1.sql', import.meta.url), 'utf8');
const invoiceEvaluationBarrier = readFileSync(new URL('../supabase/repeatable/04092026_1500_invoice_frozen_settings_evaluation_barrier_v1.sql', import.meta.url), 'utf8');
const bankingFrozenAuthority = readFileSync(new URL('../supabase/repeatable/03092026_1643_banking_pay_frozen_settings_authority_v1.sql', import.meta.url), 'utf8');
const qrRefuseServiceAcl = readFileSync(new URL('../supabase/repeatable/04092026_1710_timesheet_qr_refuse_service_acl_v1.sql', import.meta.url), 'utf8');

test('one dated resolver owns Client and Contract settings derivation', () => {
  assert.match(resolver, /_contract_settings_effective_core_v1/);
  assert.match(resolver, /CONTRACT_SETTINGS_RELEVANT_DATE_REQUIRED/);
  assert.match(resolver, /CONTRACT_SETTINGS_AUTHORITY_V1/);
  assert.match(resolver, /CONTRACT_OVERRIDE/);
  assert.match(resolver, /CLIENT_SETTINGS/);
  assert.match(resolver, /authority_fingerprint/);
  assert.match(resolver, /GLOBAL_MANUAL_PLUS_GOV_UK_ENGLAND_AND_WALES/);
  assert.match(resolver, /'erni_pct','GLOBAL_FINANCE_WINDOW'/);
  assert.match(resolver, /'apply_erni_to','GLOBAL_FINANCE_WINDOW'/);
  assert.doesNotMatch(resolver, /coalesce\(v_client\.apply_erni_to/);
});

test('Dedicated NHSP and authoritative roster policy are exact and expense-safe', () => {
  assert.match(resolver, /when v_is_nhsp then 'DEDICATED_NHSP_WEEKLY'/);
  assert.match(resolver, /v_is_nhsp or \(v_autoprocess_hr and v_no_timesheet_required\)/);
  assert.match(resolver, /'candidate_hours_view_only',v_import_authoritative/);
  assert.match(resolver, /'candidate_expense_only_carrier_required',v_import_authoritative/);
  assert.match(resolver, /'candidate_expenses_require_separate_timesheet',case when v_import_authoritative then true/);
  assert.match(resolver, /'candidate_paper_submission_enabled',case when v_import_authoritative then false/);
  assert.match(resolver, /MULTIPLE_IMPORT_FAMILIES/);
  assert.match(resolver, /ROSTER_MODE_NOT_SELECTED/);
});

test('planned weeks refresh and real Weekly and Daily Timesheets freeze at their approved boundaries', () => {
  for (const column of ['settings_authority_json', 'settings_authority_version', 'settings_authority_fingerprint', 'settings_authority_resolved_at']) {
    assert.match(migration, new RegExp(column));
  }
  assert.match(resolver, /_contract_settings_refresh_planned_for_client_v1/);
  assert.match(resolver, /_contract_settings_refresh_planned_for_contract_v1/);
  assert.match(resolver, /cw\.timesheet_id is null/);
  assert.match(resolver, /update of contract_id,week_ending_date,timesheet_id,[\s\S]*?settings_authority_json/);
  assert.match(resolver, /update of contract_id,week_ending_date,sheet_scope,[\s\S]*?settings_authority_json/);
  assert.match(resolver, /new\.sheet_scope<>'WEEKLY'::public\.timesheet_scope_enum/);
  assert.match(resolver, /v_timesheet\.sheet_scope<>'DAILY'::public\.timesheet_scope_enum/);
  assert.match(resolver, /new\.processed_at_utc is null/);
  assert.match(resolver, /if tg_op='INSERT' then[\s\S]*?new\.settings_authority_json:='\{\}'::jsonb/);
  assert.match(resolver, /tf\.processed_at_utc is not null/);
  assert.match(resolver, /settings_authority_json='\{\}'::jsonb/);
  assert.match(resolver, /return private\._timesheet_settings_authority_frozen_v1\(p_timesheet_id\)/);
  assert.match(resolver, /CONTRACT_SETTINGS_TIMESHEET_AUTHORITY_NOT_FROZEN/);
  assert.match(resolver, /v_timesheet\.sheet_scope='WEEKLY'::public\.timesheet_scope_enum/);
  assert.match(resolver, /v_unresolved_daily:=\([\s\S]*?v_workflow='DAILY'[\s\S]*?v_timesheet\.sheet_scope='DAILY'::public\.timesheet_scope_enum/);
  assert.match(resolver, /'UNRESOLVED_DAILY_SAFE'/);
  assert.match(resolver, /when coalesce\(v_week\.is_adjustment,false\) or coalesce\(v_week\.additional_seq,0\)>0[\s\S]*?then v_week\.submission_mode_snapshot/);
  assert.match(resolver, /if not coalesce\(new\.is_adjustment,false\) and coalesce\(new\.additional_seq,0\)=0 then[\s\S]*?new\.submission_mode_snapshot/);
  assert.match(resolver, /No hours, rates, financials, invoices or payment rows are/);
});

test('dated import auto-authorise never derives part of its result from today', () => {
  const datedBody = resolver.match(/create or replace function public\.import_auto_authorise_policy_resolve_v2[\s\S]*?\$function\$;/)?.[0] || '';
  assert.match(datedBody, /p_relevant_date/);
  assert.match(datedBody, /_contract_settings_effective_core_v1/);
  assert.doesNotMatch(datedBody, /import_auto_authorise_policy_resolve_v1\(/);
  assert.doesNotMatch(datedBody, /transaction_timestamp\(\)|current_date/);
});

test('legacy Client setting origin is corrected without a silent resolver fallback', () => {
  assert.match(migration, /min\(c\.start_date\) earliest_contract_date/);
  assert.match(migration, /set effective_from=ec\.earliest_contract_date/);
  assert.match(resolver, /set effective_from=v_earliest_contract_date/);
  assert.match(resolver, /CONTRACT_SETTINGS_CLIENT_SETTINGS_NOT_FOUND/);
  assert.doesNotMatch(resolver, /GLOBAL_SAFE_FALLBACK|CLIENT_SETTINGS_MISSING/);
});

test('historical Clients missing their settings canvas receive current safe defaults exactly once', () => {
  assert.match(missingSettingsCanvasRepair, /insert into public\.client_settings\(/);
  assert.match(missingSettingsCanvasRepair, /min\(c\.start_date\) as earliest_contract_date/);
  assert.match(missingSettingsCanvasRepair, /and not exists\([\s\S]*?from public\.client_settings cs where cs\.client_id=cl\.id/);
  assert.match(missingSettingsCanvasRepair, /candidate_electronic_auto_authorise_default/);
  assert.match(missingSettingsCanvasRepair, /candidate_paper_submission_enabled[\s\S]*?true/);
  assert.doesNotMatch(missingSettingsCanvasRepair, /update public\.client_settings|delete from public\.client_settings/i);
  assert.doesNotMatch(missingSettingsCanvasRepair, /total_pay|pay_method|settle|provider|Policy\.X/i);
});

test('processed Daily rows without a Contract can resolve the first historical Client settings canvas', () => {
  assert.match(processedDailyOriginRepair, /t\.sheet_scope='DAILY'::public\.timesheet_scope_enum/);
  assert.match(processedDailyOriginRepair, /tf\.processed_at_utc is not null/);
  assert.match(processedDailyOriginRepair, /t\.settings_authority_json='\{\}'::jsonb/);
  assert.match(processedDailyOriginRepair, /set effective_from=pdo\.earliest_processed_daily_date/);
  assert.match(processedDailyOriginRepair, /CLIENT_SETTINGS_PROCESSED_DAILY_ORIGIN_REPAIR_INCOMPLETE/);
  assert.doesNotMatch(processedDailyOriginRepair, /update public\.(?:timesheets|timesheets_financials)|delete from|insert into/i);
  assert.doesNotMatch(processedDailyOriginRepair, /total_pay|pay_method|settle|provider|Policy\.X/i);
});

test('invoice consumers use each real Timesheet frozen authority, not current Client or Contract settings', () => {
  for (const source of [invoiceCore, invoiceResolver, invoiceVat, invoiceCorrection, invoicePresentation, invoiceApplyEdits, invoiceGenerationFinal]) {
    assert.match(source, /_timesheet_settings_authority_frozen_v1/);
  }
  assert.doesNotMatch(invoiceCore, /from public\.client_settings/);
  assert.doesNotMatch(invoiceVat, /from public\.client_settings/);
  assert.doesNotMatch(invoiceCorrection, /from public\.client_settings/);
  assert.doesNotMatch(invoicePresentation, /from public\.client_settings/);
  assert.doesNotMatch(invoiceApplyEdits, /from public\.client_settings/);
  assert.match(invoiceApplyEdits, /\{values,daily_calc_of_invoices\}/);
  assert.match(invoiceApplyEdits, /\{values,bucket_labels_json\}/);
  assert.match(invoiceApplyEditsAcl, /revoke all on function public\.invoice_apply_edits\(uuid,jsonb,uuid\)[\s\S]*?authenticated/);
  assert.match(invoiceApplyEditsAcl, /grant execute on function public\.invoice_apply_edits\(uuid,jsonb,uuid\)[\s\S]*?to postgres,service_role/);
  assert.match(invoiceApplyEditsAcl, /revoke all on function public\.invoice_detail_get\(uuid,uuid\)[\s\S]*?authenticated/);
  assert.match(invoiceApplyEditsAcl, /grant execute on function public\.invoice_detail_get\(uuid,uuid\)[\s\S]*?to postgres,service_role/);
  const resolverBody = invoiceResolver.match(/create or replace function private\._invoice_generation_resolve_command_groups[\s\S]*?\$function\$;/)?.[0] || '';
  assert.doesNotMatch(resolverBody, /from public\.client_settings/);
  assert.match(invoiceEvaluationBarrier, /alter function private\._timesheet_settings_authority_frozen_v1\(uuid\) volatile/);
  assert.match(invoiceEvaluationBarrier, /owner to postgres/);
  assert.match(invoiceEvaluationBarrier, /revoke all on function private\._timesheet_settings_authority_frozen_v1\(uuid\)[\s\S]*?from public,anon,authenticated,service_role/);
  assert.doesNotMatch(invoiceEvaluationBarrier, /create or replace function|total_pay|pay_method|settle|provider|Policy\.X/i);
  assert.match(invoiceCore, /'settings_authority',jsonb_build_object\(/);
  assert.match(invoiceCore, /'fingerprints',jsonb_agg/);
  const currentGenerationDefinition = invoiceCore.match(/create or replace function private\._invoice_generation_advance_core_v8[\s\S]*?\$function\$;/)?.[0] || '';
  const finalGenerationDefinition = invoiceGenerationFinal.match(/create or replace function private\._invoice_generation_advance_core_v8[\s\S]*?\$function\$;/)?.[0] || '';
  assert.equal(finalGenerationDefinition, currentGenerationDefinition);
  assert.doesNotMatch(finalGenerationDefinition, /from public\.client_settings/);
  assert.match(invoiceGenerationFinal, /revoke all on function private\._invoice_generation_advance_core_v8\(jsonb,timestamptz\)[\s\S]*?authenticated/);
  assert.match(invoiceGenerationFinal, /grant execute on function private\._invoice_generation_advance_core_v8\(jsonb,timestamptz\)[\s\S]*?to postgres, service_role/);
});

test('invoice presentation release fails closed instead of certifying a deferred definition', () => {
  assert.match(invoicePresentation, /invoice_presentation_active_work/);
  assert.match(invoicePresentation, /raise exception 'INVOICE_PRESENTATION_SNAPSHOT_ACTIVE_WORK'/);
  assert.doesNotMatch(invoicePresentation, /INVOICE_PRESENTATION_SNAPSHOT_DEFERRED_ACTIVE_WORK|raise notice/);
  assert.match(invoicePresentation, /_timesheet_settings_authority_frozen_v1/);
  assert.doesNotMatch(invoicePresentation, /from public\.client_settings/);
});

test('Office summaries resolve planned and unprocessed Daily rows live but invoices require frozen Timesheet authority', () => {
  assert.match(timesheetSummary, /_contract_settings_effective_core_v1/);
  assert.match(timesheetSummary, /cw\.settings_authority_json IS NULL OR cw\.settings_authority_json='\{\}'::jsonb/);
  assert.match(invoicePrecheck, /_contract_settings_effective_core_v1/);
  assert.match(invoicePrecheck, /case when ts\.sheet_scope='DAILY'::public\.timesheet_scope_enum then 'DAILY' else 'INVOICE' end/);
  for (const source of [timesheetSummary, invoicePrecheck]) {
    assert.doesNotMatch(source, /from public\.client_settings/);
  }
  assert.match(bankingFrozenAuthority, /timesheet_summary_lightweight_rows_v1\(jsonb\)/);
  assert.match(bankingFrozenAuthority, /OFFICE_TIMESHEET_SUMMARY_LIVE_HISTORY_READ_REMAINS/);
});

test('Banking Pay changes only the source of require-reference policy to the frozen real-Timesheet authority', () => {
  assert.equal(
    (bankingFrozenAuthority.match(/v_line_ending:=case when strpos\(v_definition,E'\\r\\n'\)>0 then E'\\r\\n' else E'\\n' end;/g) || []).length,
    3,
    'all source-preserving definition rewrites must detect historical CRLF storage'
  );
  assert.doesNotMatch(bankingFrozenAuthority, /v_definition:=replace\(v_definition,E'\\r\\n',E'\\n'\)/);
  assert.equal(
    (bankingFrozenAuthority.match(/v_old:=replace\(v_old,E'\\n',v_line_ending\);/g) || []).length,
    5,
    'only exact replacement tokens may be adapted to the installed line ending'
  );
  assert.match(bankingFrozenAuthority, /pay_timesheet_impact_preview\(uuid\)/);
  assert.match(bankingFrozenAuthority, /pay_preview_candidate_collect_scope\(jsonb,uuid,jsonb,integer\)/);
  assert.match(bankingFrozenAuthority, /_timesheet_settings_authority_frozen_v1\(p_timesheet_id\)/);
  assert.match(bankingFrozenAuthority, /_timesheet_settings_authority_frozen_v1\(ts\.timesheet_id\)/);
  assert.match(bankingFrozenAuthority, /BANKING_PAY_TIMESHEET_IMPACT_SOURCE_DRIFT/);
  assert.match(bankingFrozenAuthority, /BANKING_PAY_COLLECT_SCOPE_SOURCE_DRIFT/);
  assert.doesNotMatch(bankingFrozenAuthority, /total_pay|pay_method|settle|provider|Policy.X/i);
});

test('the historical QR refusal helper is reachable only through its guarded service caller', () => {
  assert.match(qrRefuseServiceAcl, /revoke all on function public\.timesheet_qr_refuse_and_reset\(uuid,uuid,text,uuid\)[\s\S]*?authenticated/);
  assert.match(qrRefuseServiceAcl, /if exists \([\s\S]*?pg_catalog\.pg_roles[\s\S]*?rolname='authenticator'[\s\S]*?execute 'revoke all on function public\.timesheet_qr_refuse_and_reset\(uuid,uuid,text,uuid\) from authenticator'/);
  assert.match(qrRefuseServiceAcl, /if exists \([\s\S]*?pg_catalog\.pg_roles[\s\S]*?rolname='supabase_admin'[\s\S]*?execute 'revoke all on function public\.timesheet_qr_refuse_and_reset\(uuid,uuid,text,uuid\) from supabase_admin'/);
  assert.match(qrRefuseServiceAcl, /grant execute on function public\.timesheet_qr_refuse_and_reset\(uuid,uuid,text,uuid\)[\s\S]*?to postgres,service_role/);
  assert.doesNotMatch(qrRefuseServiceAcl, /create or replace function|update |insert |delete |total_pay|pay_method|settle|provider|Policy\.X/i);
  assert.match(worker, /requireUser\(env, req, \['admin'\]\)[\s\S]*?timesheet_qr_refuse_and_reset/);
});

test('Worker consumers use the central resolver and fail closed', () => {
  assert.match(worker, /sbRpc\(env, 'contract_settings_effective_get_v1'/);
  assert.match(worker, /CONTRACT_SETTINGS_AUTHORITY_INVALID/);
  assert.match(worker, /contract_settings_resolver_fail_closed/);
  assert.match(worker, /is_import_authoritative:\s*true/);
  assert.match(worker, /settings_bank_holiday_feed_refresh_claim_v1/);
  assert.match(worker, /https:\/\/www\.gov\.uk\/bank-holidays\.json/);
  assert.match(worker, /claim\.division !== 'england-and-wales'/);
  assert.doesNotMatch(worker, /cs\?\.apply_erni_to\s*\|\|\s*finance/);
});

test('Client-facing writes cannot restore legacy Client ERNI or Bank Holiday ownership', () => {
  const createFields = worker.match(/const CLIENT_SETTINGS_CREATE_FIELDS = new Set\(\[([\s\S]*?)\]\);/)?.[1] || '';
  assert.doesNotMatch(createFields, /erni_pct|apply_erni_to|bh_source|bh_list|bh_feed_url/);
  assert.match(worker, /delete csInput\.erni_pct/);
  assert.match(worker, /delete csInput\.apply_erni_to/);
  assert.match(worker, /delete csInput\.bh_source/);
  assert.match(worker, /delete csInput\.bh_list/);
  assert.match(worker, /delete csInput\.bh_feed_url/);
});

test('TEST V8 cutover cannot silently reactivate the retained legacy invoice generator', () => {
  const testVars = wrangler.match(/\[env\.test\.vars\]([\s\S]*?)(?=\n\[|$)/)?.[1] || '';
  assert.match(testVars, /INVOICE_ASYNC_PIPELINE_ENABLED\s*=\s*"true"/);
  assert.match(testVars, /INVOICE_DOCUMENT_PROCESSOR_ENABLED\s*=\s*"true"/);
  assert.match(testVars, /INVOICE_ASYNC_SCHEDULED_ENABLED\s*=\s*"false"/);

  const retired = invoiceAsyncHttp.match(/function isRetiredInvoiceLegacyRoute[\s\S]*?\n\}/)?.[0] || '';
  for (const route of [
    '/api/invoices',
    '/api/invoices/tsfin/by-week',
    '/api/invoices/create-expenses',
    '/api/nhsp/invoices/run',
    '/api/invpdf/queue/drain',
    '/api/tspdf/queue/drain'
  ]) {
    assert.match(retired, new RegExp(route.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')));
  }
  assert.match(invoiceAsyncHttp, /INVOICE_LEGACY_ROUTE_RETIRED/);

  const asyncRouterPosition = worker.indexOf('const invoiceAsyncResponse = await handleInvoiceAsyncHttpRequest');
  const legacyRouterPosition = worker.indexOf("p === '/api/invoices')");
  assert.ok(asyncRouterPosition >= 0 && legacyRouterPosition > asyncRouterPosition);

  const scheduledStart = worker.lastIndexOf('async scheduled(event, env, ctx)');
  const scheduledBody = scheduledStart >= 0 ? worker.slice(scheduledStart) : '';
  assert.match(scheduledBody, /runAutoInvoiceCycleAsync/);
  assert.doesNotMatch(scheduledBody, /runAutoInvoiceCycle\(env/);
  assert.doesNotMatch(scheduledBody, /invoice_generate_from_outbox_batch/);
});
