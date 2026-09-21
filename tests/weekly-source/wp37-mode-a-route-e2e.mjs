// Weekly Source Plan 6.2 — WP-37 Mode A route proof.
//
//   node tests/weekly-source/wp37-mode-a-route-e2e.mjs
//
// Environment: PSQL_BIN, PGPASSWORD (local throwaway), PGOPTIONS='-c jit=off',
// WP37_DATABASE (default `wp37_route`).  In Git Bash also export
// MSYS_NO_PATHCONV=1.
//
// WHY THIS FILE EXISTS.  WP-31's hostile review of WP-04 found (finding F2)
// that `public.weekly_source_mode_a_reference_apply_atomic_v1` had **no
// caller**: `/uploads/accept` dispatched and stopped, so after two
// `EXACT_MATCH` comparisons the reference was never written, no reference-apply
// operation existed and the signed Timesheet stayed `STORED`.  The only route
// that did reach the rule was the ordinary Imports screen, which bypasses the
// Weekly Source ledger.  That is the third time this programme has shipped an
// owner with no caller, so a static assertion is worthless here: this file
// drives the REAL Office HTTP route — the same exported
// `dispatchWeeklySourceRequest` that `broker/src/index.js` mounts — over real
// HTTP into a real PostgreSQL 17.11 build, and then measures the database with
// an independent query.
//
// The only substitutions are the two dependencies the Worker itself injects at
// `broker/src/index.js:199412-199416`:
//   * `loadFileBytes`, because this proof does not exercise R2; and
//   * `acceptUpload`, which returns the publication the fixture published
//     through the real `public.weekly_source_projection_rows_apply_atomic_v1`
//     owner.  Its `publication_id` is the whole of its contribution to the Mode
//     A journey, and F2 is entirely about what the route does AFTER it.
// PostgREST itself, which this cluster does not run, is served from the same
// database by `tests/weekly-source/wp23-postgrest-local.mjs`, one transaction
// per request, as `service_role`.  Everything else is the shipped broker code.
//
// Safety: local disposable database only, never a hosted target, never LIVE.

import { spawnSync } from 'node:child_process';

import { dispatchWeeklySourceRequest } from '../../broker/src/weekly-source/routes.js';
import { weeklySourceModeAInternals } from '../../broker/src/index.js';
import { createLocalPostgrest } from './wp23-postgrest-local.mjs';

const DATABASE = process.env.WP37_DATABASE || 'wp37_route';
const postgrest = createLocalPostgrest({ database: DATABASE });
let ORIGIN = '';

const results = [];
function record(id, owner, passed, detail) {
  results.push({ id, owner, result: passed ? 'PASS' : 'FAIL', detail: String(detail) });
  process.stdout.write(`WP37|${id}|${passed ? 'PASS' : 'FAIL'}|${owner}|${String(detail)}\n`);
}

function query(sql) {
  const run = spawnSync(process.env.PSQL_BIN || 'psql', [
    '-X', '-tA', '-v', 'ON_ERROR_STOP=1',
    `postgresql://postgres@127.0.0.1:55433/${DATABASE}`, '-c', sql,
  ], {
    encoding: 'utf8',
    env: { ...process.env, PGPASSWORD: process.env.PGPASSWORD || 'localonly' },
    maxBuffer: 32 * 1024 * 1024,
  });
  if (run.status !== 0) throw new Error(`measurement failed: ${run.stderr || run.stdout}`);
  return String(run.stdout).trim();
}
const count = (sql) => Number(query(sql));

// The broker's own transport, reproduced exactly (see wp23-transport-e2e.mjs).
async function sbRpcLocal(functionName, args) {
  const response = await fetch(`${ORIGIN}/rest/v1/rpc/${encodeURIComponent(functionName)}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', apikey: 'local', authorization: 'Bearer local' },
    body: JSON.stringify(args || {}),
  });
  const text = await response.text();
  let json = null;
  try { json = text ? JSON.parse(text) : null; } catch { json = null; }
  if (!response.ok) {
    const error = new Error(`RPC ${functionName} failed ${response.status}: ${text}`);
    error.status = response.status;
    error.body = text;
    error.json = json;
    error.fn = functionName;
    throw error;
  }
  return json;
}

async function main() {
  const fixture = query(
    `select actor_user_id||'|'||publication_id||'|'||coalesce(timesheet_id::text,'')
       from public.wp37_route_fixture where code='E2E'`,
  ).split('|');
  const [ACTOR, PUBLICATION_ID, TIMESHEET_ID] = fixture;
  if (!ACTOR || !PUBLICATION_ID) throw new Error('the WP-37 route fixture is missing');

  ORIGIN = await postgrest.listen();
  process.stdout.write(`WP37 Mode A route proof against ${DATABASE} via ${ORIGIN}\n\n`);

  // The same Worker environment shape `broker/src/index.js` builds, so the
  // established post-commit follow-up reaches this database through the same
  // `/rest/v1` transport the broker uses in production.
  const brokerEnv = {
    SUPABASE_URL: ORIGIN,
    SUPABASE_SERVICE_ROLE_KEY: 'local-service-role',
    SUPABASE_ANON_KEY: 'local-anon',
  };

  const acceptCalls = [];
  const dependencies = {
    requireUser: async () => ({ id: ACTOR, role: 'admin' }),
    rpc: (functionName, args) => sbRpcLocal(functionName, args),
    // The shipped runner object itself, not a stand-in.
    runImportReviewPostCommit: (runtimeEnv, details) =>
      weeklySourceModeAInternals.runImportReviewPostCommit(runtimeEnv ?? brokerEnv, details),
    loadFileBytes: async () => new TextEncoder().encode('wp37-mode-a-source-file'),
    acceptUpload: async ({ body }) => {
      acceptCalls.push(body?.file_key ?? null);
      return {
        ok: true,
        status: 'CURRENT',
        upload_id: query(
          `select upload_id from public.weekly_source_projection_publications
            where id='${PUBLICATION_ID}'`,
        ),
        authority_scope_version: 1,
        publication_id: PUBLICATION_ID,
        idempotent: false,
        source_profile: 'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1',
      };
    },
  };

  const driveAccept = async () => {
    const response = await dispatchWeeklySourceRequest(
      new Request('https://wp37.invalid/api/weekly-source/v1/uploads/accept', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ file_key: 'wp37/mode-a.csv' }),
      }),
      brokerEnv, {}, dependencies,
    );
    let payload = null;
    try { payload = await response.clone().json(); } catch { payload = null; }
    return { status: response.status, payload };
  };

  const operationsSql = `select count(*) from public.weekly_timesheet_reference_apply_operations
    where projection_publication_id='${PUBLICATION_ID}'`;
  const itemsSql = `select count(*) from public.weekly_timesheet_reference_apply_items item
    join public.weekly_timesheet_reference_apply_operations operation on operation.id=item.operation_id
    where operation.projection_publication_id='${PUBLICATION_ID}'`;
  const referenceSql = `select count(*) from public.nhsp_shifts
    where timesheet_id='${TIMESHEET_ID}' and nullif(btrim(coalesce(ref_num,'')),'') is not null`;
  // WP-31 measured authorisation exactly here: `timesheets.status` is the
  // submission status and stays `STORED` on this journey, while
  // `timesheets.authorised_at_server` (with the current financial row's
  // `processing_status`) is the authorisation itself. Its before-reading was
  // "timesheet status=STORED, authorised_at_server=<NULL>".
  const statusSql = `select coalesce(status::text,'?')
      ||'/authorised_at_server='||coalesce(authorised_at_server::text,'NULL')
    from public.timesheets where timesheet_id='${TIMESHEET_ID}'`;
  const authorisedSql = `select count(*) from public.timesheets
    where timesheet_id='${TIMESHEET_ID}' and authorised_at_server is not null`;
  const tsfinSql = `select coalesce(max(processing_status::text),'<no financial row>')
    from public.timesheets_financials
    where timesheet_id='${TIMESHEET_ID}' and is_current`;

  const before = {
    operations: count(operationsSql),
    items: count(itemsSql),
    references: count(referenceSql),
    status: query(statusSql),
    authorised: count(authorisedSql),
    tsfin: query(tsfinSql),
  };

  const accepted = await driveAccept();

  const after = {
    operations: count(operationsSql),
    items: count(itemsSql),
    references: count(referenceSql),
    status: query(statusSql),
    authorised: count(authorisedSql),
    tsfin: query(tsfinSql),
    comparisons: count(`select count(*) from public.weekly_timesheet_source_comparisons
      where projection_publication_id='${PUBLICATION_ID}' and comparison_state='EXACT_MATCH'`),
    applied: count(`select count(*) from public.weekly_timesheet_reference_apply_operations
      where projection_publication_id='${PUBLICATION_ID}' and state='APPLIED'`),
    autoApplied: count(`select count(*) from public.weekly_timesheet_reference_apply_operations
      where projection_publication_id='${PUBLICATION_ID}' and auto_authorise_applied`),
  };

  record(
    'WP37-F2-ROUTE',
    'POST /api/weekly-source/v1/uploads/accept -> weekly_source_mode_a_reference_apply_atomic_v1',
    accepted.status === 200 && after.operations > 0 && after.items > 0
      && after.references > 0 && before.authorised === 0 && after.authorised === 1,
    `HTTP ${accepted.status}; acceptUpload calls=${acceptCalls.length}; `
    + `mode_a=${JSON.stringify(accepted.payload?.mode_a ?? null)}; `
    + `EXACT_MATCH comparisons=${after.comparisons}; `
    + `reference_apply_operations ${before.operations} -> ${after.operations} `
    + `(APPLIED=${after.applied}, auto_authorise_applied=${after.autoApplied}); `
    + `items ${before.items} -> ${after.items}; `
    + `nhsp_shifts with ref_num ${before.references} -> ${after.references}; `
    + `timesheet ${before.status} -> ${after.status}; `
    + `current financial row ${before.tsfin} -> ${after.tsfin}`,
  );

  // Negative: this journey is an exact match on every day, so the established
  // owner must raise no manager-correction email work at all. Nothing in this
  // proof sends or enqueues an email.
  const emailWork = count(`select count(*) from public.import_review_decisions decision
      join public.hr_imports import_row on import_row.id=decision.import_id
      where import_row.parser_version='WEEKLY_SOURCE_MODE_A_BRIDGE_V1'
        and decision.is_current and decision.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')`);
  record(
    'WP37-F2-NO-EMAIL', 'exact coverage raises no manager email',
    emailWork === 0,
    `current EMAIL_ISSUE / EMAIL_REMINDER decisions on the bridged import = ${emailWork}`,
  );

  // Re-accepting the same file must be idempotent: the established owner and
  // the Weekly Source ledger both key on the publication and the Timesheet
  // revision, so a second drive adds no operation and no item.
  const replay = await driveAccept();
  const afterReplay = {
    operations: count(operationsSql),
    items: count(itemsSql),
    references: count(referenceSql),
  };
  record(
    'WP37-F2-REPLAY', 'route idempotency',
    replay.status === 200
      && afterReplay.operations === after.operations
      && afterReplay.items === after.items
      && afterReplay.references === after.references,
    `second POST -> ${replay.status}; operations ${after.operations} -> ${afterReplay.operations}; `
    + `items ${after.items} -> ${afterReplay.items}; references ${after.references} -> ${afterReplay.references}`,
  );

  // Negative: nothing on this route may cross into the source-authority route.
  const financial = count('select count(*) from public.weekly_source_billing_movements')
    + count('select count(*) from public.weekly_source_final_revisions')
    + count('select count(*) from public.weekly_source_charge_checks');
  record(
    'WP37-F2-BOUNDARY', 'Mode A never crosses into source authority',
    financial === 0,
    `billing movements + final revisions + charge checks = ${financial}`,
  );

  // WP-31 finding F5, REPORTED not fixed: the publication is committed by
  // `acceptUpload` before Mode A runs, so a dispatcher refusal still reaches
  // Office as a failure of an upload that is already CURRENT. Making the two
  // atomic needs one owner and is not this package's change. What IS fixed is
  // that the refusal now names the publication and its state, so the error
  // cannot be read as a failed publication. Driven here, not described.
  query(`update public.client_settings set no_timesheet_required=true
    where client_id=(select client_id from public.wp37_route_fixture where code='E2E')`);
  const ineligible = await driveAccept();
  const publicationState = query(
    `select state from public.weekly_source_projection_publications where id='${PUBLICATION_ID}'`,
  );
  query(`update public.client_settings set no_timesheet_required=false
    where client_id=(select client_id from public.wp37_route_fixture where code='E2E')`);
  record(
    'WP37-F5-REFUSAL-NAMES-PUBLICATION', 'dispatch refusal after a committed publication',
    ineligible.payload?.error_code === 'WEEKLY_SOURCE_MODE_A_DISPATCH_FAILED'
      && ineligible.payload?.details?.publication_id === PUBLICATION_ID
      && ineligible.payload?.details?.publication_state === 'CURRENT'
      && /ROUTE_NOT_ELIGIBLE/.test(String(ineligible.payload?.details?.reason || ''))
      && publicationState === 'CURRENT',
    `HTTP ${ineligible.status} ${ineligible.payload?.error_code}; `
    + `details=${JSON.stringify(ineligible.payload?.details ?? null)}; `
    + `publication remains ${publicationState}`,
  );

  await postgrest.close();
  const failed = results.filter((row) => row.result === 'FAIL');
  process.stdout.write(`\nWP37 route proof: ${results.length - failed.length}/${results.length} passed\n`);
  if (failed.length) process.exitCode = 1;
}

main().catch(async (error) => {
  try { await postgrest.close(); } catch { /* the server may never have opened */ }
  process.stderr.write(`WP37 route proof failed: ${error?.stack || error}\n`);
  process.exitCode = 1;
});
