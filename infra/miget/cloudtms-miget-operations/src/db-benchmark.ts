import { timingSafeEqual } from "node:crypto";
import { Client, type QueryResultRow } from "pg";

export const DB_BENCHMARK_PREFIX = "/__debug/db-benchmark";

const POSTGREST_TOKEN_HEADER = "x-cloudtms-benchmark-postgrest-token";
const MAX_POSTGREST_RESPONSE_BYTES = 4 * 1_048_576;
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const SETTINGS_COLUMNS = [
  "agency_name",
  "agency_logo",
  "timesheet_header_json",
  "timesheet_footer_json",
  "temporary_worker_declaration_json",
  "client_declaration_json",
] as const;

type BenchmarkPath = "postgrest" | "hyperdrive";
type BenchmarkTest = "select_1" | "small_read" | "multi_read";
type HyperdriveVariant = "sequential" | "optimized";

export interface DbBenchmarkEnv {
  HYPERDRIVE: Hyperdrive;
  MIGET_MCP_ROUTE_TOKEN: string;
  MIGET_POSTGREST_ORIGIN: string;
}

interface PayloadSummary {
  row_count: number;
  query_count: number;
  result_bytes: number;
  shape_hash: string;
}

interface PathMeasurement extends PayloadSummary {
  environment: "TEST";
  path: BenchmarkPath;
  test: BenchmarkTest;
  variant: "current" | HyperdriveVariant;
  cache_mode: "disabled";
  placement_mode: "targeted";
  handler_elapsed_ms?: number;
  db_elapsed_ms: number;
  connect_elapsed_ms: number | null;
  query_elapsed_ms: number;
  subrequest_elapsed_ms: number | null;
  query_timings_ms: number[];
}

interface PostgrestStep {
  payload: unknown;
  bytes: number;
  elapsedMs: number;
}

class BenchmarkHttpError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

export function isDbBenchmarkPath(pathname: string): boolean {
  return pathname === DB_BENCHMARK_PREFIX || pathname.startsWith(`${DB_BENCHMARK_PREFIX}/`);
}

function milliseconds(startedAt: number): number {
  return Number((performance.now() - startedAt).toFixed(3));
}

function authorized(request: Request, env: DbBenchmarkEnv): boolean {
  const expected = String(env.MIGET_MCP_ROUTE_TOKEN || "");
  const supplied = request.headers.get("authorization")?.match(/^Bearer\s+(.+)$/i)?.[1] ?? "";
  if (!expected || supplied.length !== expected.length) return false;
  return timingSafeEqual(Buffer.from(supplied), Buffer.from(expected));
}

function benchmarkResponse(payload: unknown, status = 200): Response {
  return Response.json(payload, {
    status,
    headers: {
      "cache-control": "no-store, max-age=0",
      "x-content-type-options": "nosniff",
      "x-cloudtms-environment": "TEST",
    },
  });
}

function decodeJwtRole(token: string): string | null {
  try {
    const encoded = token.split(".")[1];
    if (!encoded) return null;
    const normalized = encoded.replace(/-/g, "+").replace(/_/g, "/");
    const padded = normalized.padEnd(Math.ceil(normalized.length / 4) * 4, "=");
    const bytes = Uint8Array.from(atob(padded), (character) => character.charCodeAt(0));
    const payload = JSON.parse(new TextDecoder().decode(bytes)) as unknown;
    if (!payload || typeof payload !== "object" || Array.isArray(payload)) return null;
    const role = (payload as Record<string, unknown>).role;
    return typeof role === "string" ? role : null;
  } catch {
    return null;
  }
}

function postgrestToken(request: Request): string {
  const token = request.headers.get(POSTGREST_TOKEN_HEADER)?.trim() ?? "";
  if (!token) {
    throw new BenchmarkHttpError(401, "A TEST PostgREST service token is required");
  }
  if (decodeJwtRole(token) !== "service_role") {
    throw new BenchmarkHttpError(403, "The supplied PostgREST token is not a service_role token");
  }
  return token;
}

function requestedTest(url: URL): BenchmarkTest {
  const value = url.searchParams.get("test") ?? "select_1";
  if (value === "select_1" || value === "small_read" || value === "multi_read") return value;
  throw new BenchmarkHttpError(400, "Unsupported benchmark test");
}

function requestedVariant(url: URL): HyperdriveVariant {
  const value = url.searchParams.get("variant") ?? "sequential";
  if (value === "sequential" || value === "optimized") return value;
  throw new BenchmarkHttpError(400, "Unsupported Hyperdrive benchmark variant");
}

function requestedTimesheetId(url: URL, test: BenchmarkTest): string | null {
  if (test !== "multi_read") return null;
  const value = url.searchParams.get("timesheet_id")?.trim() ?? "";
  if (!UUID_PATTERN.test(value)) {
    throw new BenchmarkHttpError(400, "A valid TEST timesheet_id is required for multi_read");
  }
  return value;
}

function jsonBytes(value: unknown): number {
  return new TextEncoder().encode(JSON.stringify(value)).byteLength;
}

function shapeOf(value: unknown, depth = 0): unknown {
  if (value === null) return "null";
  if (value instanceof Date) return "date";
  if (Array.isArray(value)) {
    return {
      type: "array",
      length: value.length,
      item: value.length > 0 && depth < 4 ? shapeOf(value[0], depth + 1) : "empty",
    };
  }
  if (typeof value === "object") {
    if (depth >= 4) return "object";
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, child]) => [key, shapeOf(child, depth + 1)]),
    );
  }
  return typeof value;
}

async function shapeHash(value: unknown): Promise<string> {
  const encoded = new TextEncoder().encode(JSON.stringify(shapeOf(value)));
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", encoded));
  return Array.from(digest, (byte) => byte.toString(16).padStart(2, "0")).join("").slice(0, 16);
}

async function payloadSummary(
  value: unknown,
  queryCount: number,
  rowCount: number,
  measuredBytes?: number,
): Promise<PayloadSummary> {
  return {
    row_count: rowCount,
    query_count: queryCount,
    result_bytes: measuredBytes ?? jsonBytes(value),
    shape_hash: await shapeHash(value),
  };
}

function rowsFrom(payload: unknown): Array<Record<string, unknown>> {
  if (!Array.isArray(payload)) return [];
  return payload.filter(
    (row): row is Record<string, unknown> => typeof row === "object" && row !== null && !Array.isArray(row),
  );
}

function firstRow(payload: unknown): Record<string, unknown> | null {
  return rowsFrom(payload)[0] ?? null;
}

async function postgrestJson(
  env: DbBenchmarkEnv,
  token: string,
  pathAndQuery: string,
  init: { method?: "GET" | "POST"; body?: string } = {},
): Promise<PostgrestStep> {
  const url = `${env.MIGET_POSTGREST_ORIGIN.replace(/\/$/, "")}${pathAndQuery}`;
  const headers = new Headers({
    Accept: "application/json",
    Authorization: `Bearer ${token}`,
    apikey: token,
    "cache-control": "no-cache",
  });
  if (init.body !== undefined) headers.set("content-type", "application/json");

  const startedAt = performance.now();
  const response = await fetch(url, {
    method: init.method ?? "GET",
    headers,
    body: init.body,
    cache: "no-store",
    redirect: "manual",
  });
  const elapsedMs = milliseconds(startedAt);
  const body = await response.arrayBuffer();
  if (body.byteLength > MAX_POSTGREST_RESPONSE_BYTES) {
    throw new BenchmarkHttpError(502, "PostgREST benchmark response exceeded the safety limit");
  }
  if (!response.ok) {
    throw new BenchmarkHttpError(502, `PostgREST benchmark request returned HTTP ${response.status}`);
  }
  return {
    payload: JSON.parse(new TextDecoder().decode(body)) as unknown,
    bytes: body.byteLength,
    elapsedMs,
  };
}

async function runPostgrest(
  env: DbBenchmarkEnv,
  token: string,
  test: BenchmarkTest,
  timesheetId: string | null,
): Promise<PathMeasurement> {
  const dbStartedAt = performance.now();
  const timings: number[] = [];
  let queryCount = 0;
  let resultBytes = 0;

  const step = async (
    path: string,
    init?: { method?: "GET" | "POST"; body?: string },
  ): Promise<unknown> => {
    const result = await postgrestJson(env, token, path, init);
    queryCount += 1;
    resultBytes += result.bytes;
    timings.push(result.elapsedMs);
    return result.payload;
  };

  let payload: unknown;
  let rowCount = 0;

  if (test === "select_1") {
    payload = await step("/rpc/codex_debug_select_sql", {
      method: "POST",
      body: JSON.stringify({ p_sql: "select 1::integer as value", p_limit: 1 }),
    });
    rowCount = 1;
  } else if (test === "small_read") {
    payload = await step(
      `/settings_defaults?id=eq.1&select=${SETTINGS_COLUMNS.join(",")}&limit=1`,
    );
    rowCount = rowsFrom(payload).length;
  } else {
    if (!timesheetId) throw new BenchmarkHttpError(400, "timesheet_id is required");
    const encodedId = encodeURIComponent(timesheetId);
    const timesheetPayload = await step(
      `/timesheets?timesheet_id=eq.${encodedId}&is_current=eq.true&select=*&limit=1`,
    );
    const timesheet = firstRow(timesheetPayload);
    if (!timesheet) throw new BenchmarkHttpError(404, "The TEST benchmark timesheet was not found");

    let summary: Record<string, unknown> | null = null;
    if (!timesheet.contract_id) {
      summary = firstRow(
        await step(
          `/v_timesheets_summary?timesheet_id=eq.${encodedId}` +
            "&select=timesheet_id,candidate_id,client_id,candidate_name,client_name,contract_id&limit=1",
        ),
      );
    }

    const financial = firstRow(
      await step(
        `/timesheets_financials?timesheet_id=eq.${encodedId}&is_current=eq.true&select=*&limit=1`,
      ),
    );
    const contractId = String(timesheet.contract_id ?? summary?.contract_id ?? "");
    const contract = contractId
      ? firstRow(await step(`/contracts?id=eq.${encodeURIComponent(contractId)}&select=*&limit=1`))
      : null;
    const clientId = String(contract?.client_id ?? summary?.client_id ?? "");
    const candidateId = String(contract?.candidate_id ?? summary?.candidate_id ?? "");
    const client = clientId
      ? firstRow(await step(`/clients?id=eq.${encodeURIComponent(clientId)}&select=*&limit=1`))
      : null;
    const candidate = candidateId
      ? firstRow(await step(`/candidates?id=eq.${encodeURIComponent(candidateId)}&select=*&limit=1`))
      : null;
    const settings = firstRow(
      await step(`/settings_defaults?id=eq.1&select=${SETTINGS_COLUMNS.join(",")}&limit=1`),
    );

    payload = { timesheet, summary, financial, contract, client, candidate, settings };
    rowCount = Object.values(payload as Record<string, unknown>).filter((value) => value !== null).length;
  }

  const dbElapsedMs = milliseconds(dbStartedAt);
  const summary = await payloadSummary(payload, queryCount, rowCount, resultBytes);
  return {
    environment: "TEST",
    path: "postgrest",
    test,
    variant: "current",
    cache_mode: "disabled",
    placement_mode: "targeted",
    db_elapsed_ms: dbElapsedMs,
    connect_elapsed_ms: null,
    query_elapsed_ms: Number(timings.reduce((total, value) => total + value, 0).toFixed(3)),
    subrequest_elapsed_ms: dbElapsedMs,
    query_timings_ms: timings,
    ...summary,
  };
}

async function runHyperdrive(
  env: DbBenchmarkEnv,
  test: BenchmarkTest,
  variant: HyperdriveVariant,
  timesheetId: string | null,
): Promise<PathMeasurement> {
  const dbStartedAt = performance.now();
  const client = new Client({
    connectionString: env.HYPERDRIVE.connectionString,
    application_name: "cloudtms-hyperdrive-benchmark-test-only",
    connectionTimeoutMillis: 8_000,
    query_timeout: 8_000,
  });
  const connectStartedAt = performance.now();
  await client.connect();
  const connectElapsedMs = milliseconds(connectStartedAt);
  const timings: number[] = [];
  let queryCount = 0;

  const query = async <T extends QueryResultRow>(sql: string, values: unknown[] = []): Promise<T[]> => {
    const startedAt = performance.now();
    const result = await client.query<T>(sql, values);
    timings.push(milliseconds(startedAt));
    queryCount += 1;
    return result.rows;
  };

  let payload: unknown;
  let rowCount = 0;

  if (test === "select_1") {
    payload = await query<{ value: number }>("select 1::integer as value");
    rowCount = (payload as unknown[]).length;
  } else if (test === "small_read") {
    payload = await query(
      `select ${SETTINGS_COLUMNS.join(", ")}
       from public.settings_defaults
       where id = $1::smallint
       limit 1`,
      [1],
    );
    rowCount = (payload as unknown[]).length;
  } else if (variant === "optimized") {
    if (!timesheetId) throw new BenchmarkHttpError(400, "timesheet_id is required");
    const rows = await query<{ payload: Record<string, unknown> }>(
      `with selected_timesheet as (
         select t.*
         from public.timesheets t
         where t.timesheet_id = $1::uuid
           and t.is_current is true
         limit 1
       )
       select jsonb_build_object(
         'timesheet', to_jsonb(t),
         'summary', case when s.timesheet_id is null then null else jsonb_build_object(
           'timesheet_id', s.timesheet_id,
           'candidate_id', s.candidate_id,
           'client_id', s.client_id,
           'candidate_name', s.candidate_name,
           'client_name', s.client_name,
           'contract_id', s.contract_id
         ) end,
         'financial', case when f.id is null then null else to_jsonb(f) end,
         'contract', case when c.id is null then null else to_jsonb(c) end,
         'client', case when cl.id is null then null else to_jsonb(cl) end,
         'candidate', case when ca.id is null then null else to_jsonb(ca) end,
         'settings', case when d.id is null then null else jsonb_build_object(
           'agency_name', d.agency_name,
           'agency_logo', d.agency_logo,
           'timesheet_header_json', d.timesheet_header_json,
           'timesheet_footer_json', d.timesheet_footer_json,
           'temporary_worker_declaration_json', d.temporary_worker_declaration_json,
           'client_declaration_json', d.client_declaration_json
         ) end
       ) as payload
       from selected_timesheet t
       left join lateral (
         select summary.timesheet_id, summary.candidate_id, summary.client_id,
                summary.candidate_name, summary.client_name, summary.contract_id
         from public.v_timesheets_summary summary
         where t.contract_id is null
           and summary.timesheet_id = t.timesheet_id
         limit 1
       ) s on true
       left join lateral (
         select financial.*
         from public.timesheets_financials financial
         where financial.timesheet_id = t.timesheet_id
           and financial.is_current is true
         limit 1
       ) f on true
       left join lateral (
         select contract_row.*
         from public.contracts contract_row
         where contract_row.id = coalesce(t.contract_id, s.contract_id)
         limit 1
       ) c on true
       left join lateral (
         select client_row.*
         from public.clients client_row
         where client_row.id = coalesce(c.client_id, s.client_id)
         limit 1
       ) cl on true
       left join lateral (
         select candidate_row.*
         from public.candidates candidate_row
         where candidate_row.id = coalesce(c.candidate_id, s.candidate_id)
         limit 1
       ) ca on true
       left join lateral (
         select defaults.*
         from public.settings_defaults defaults
         where defaults.id = 1
         limit 1
       ) d on true`,
      [timesheetId],
    );
    payload = rows[0]?.payload ?? null;
    if (!payload) throw new BenchmarkHttpError(404, "The TEST benchmark timesheet was not found");
    rowCount = Object.values(payload as Record<string, unknown>).filter((value) => value !== null).length;
  } else {
    if (!timesheetId) throw new BenchmarkHttpError(400, "timesheet_id is required");
    const timesheet = (
      await query<Record<string, unknown>>(
        `select * from public.timesheets
         where timesheet_id = $1::uuid and is_current is true
         limit 1`,
        [timesheetId],
      )
    )[0] ?? null;
    if (!timesheet) throw new BenchmarkHttpError(404, "The TEST benchmark timesheet was not found");

    const summary = !timesheet.contract_id
      ? (
          await query<Record<string, unknown>>(
            `select timesheet_id, candidate_id, client_id, candidate_name, client_name, contract_id
             from public.v_timesheets_summary
             where timesheet_id = $1::uuid
             limit 1`,
            [timesheetId],
          )
        )[0] ?? null
      : null;
    const financial = (
      await query<Record<string, unknown>>(
        `select * from public.timesheets_financials
         where timesheet_id = $1::uuid and is_current is true
         limit 1`,
        [timesheetId],
      )
    )[0] ?? null;
    const contractId = String(timesheet.contract_id ?? summary?.contract_id ?? "");
    const contract = contractId
      ? (await query<Record<string, unknown>>("select * from public.contracts where id = $1::uuid limit 1", [contractId]))[0] ?? null
      : null;
    const clientId = String(contract?.client_id ?? summary?.client_id ?? "");
    const candidateId = String(contract?.candidate_id ?? summary?.candidate_id ?? "");
    const clientRow = clientId
      ? (await query<Record<string, unknown>>("select * from public.clients where id = $1::uuid limit 1", [clientId]))[0] ?? null
      : null;
    const candidate = candidateId
      ? (await query<Record<string, unknown>>("select * from public.candidates where id = $1::uuid limit 1", [candidateId]))[0] ?? null
      : null;
    const settings = (
      await query<Record<string, unknown>>(
        `select ${SETTINGS_COLUMNS.join(", ")} from public.settings_defaults where id = $1::smallint limit 1`,
        [1],
      )
    )[0] ?? null;

    payload = { timesheet, summary, financial, contract, client: clientRow, candidate, settings };
    rowCount = Object.values(payload as Record<string, unknown>).filter((value) => value !== null).length;
  }

  const summary = await payloadSummary(payload, queryCount, rowCount);
  const queryElapsedMs = Number(timings.reduce((total, value) => total + value, 0).toFixed(3));
  return {
    environment: "TEST",
    path: "hyperdrive",
    test,
    variant,
    cache_mode: "disabled",
    placement_mode: "targeted",
    db_elapsed_ms: milliseconds(dbStartedAt),
    connect_elapsed_ms: connectElapsedMs,
    query_elapsed_ms: queryElapsedMs,
    subrequest_elapsed_ms: null,
    query_timings_ms: timings,
    ...summary,
  };
}

async function benchmarkMetadata(request: Request, env: DbBenchmarkEnv): Promise<Response> {
  const client = new Client({
    connectionString: env.HYPERDRIVE.connectionString,
    application_name: "cloudtms-hyperdrive-benchmark-metadata-test-only",
    connectionTimeoutMillis: 8_000,
    query_timeout: 8_000,
  });
  await client.connect();
  const roleResult = await client.query<{
    expected_database: boolean;
    superuser: boolean;
    bypass_rls: boolean;
    owns_timesheets: boolean;
    row_security: string;
    can_select_timesheets: boolean;
  }>(`
    select
      current_database() = 'cloudtms_test_clone' as expected_database,
      r.rolsuper as superuser,
      r.rolbypassrls as bypass_rls,
      c.relowner = r.oid as owns_timesheets,
      current_setting('row_security') as row_security,
      has_table_privilege(current_user, 'public.timesheets', 'select') as can_select_timesheets
    from pg_roles r
    join pg_class c on c.oid = 'public.timesheets'::regclass
    where r.rolname = current_user
  `);
  const sessionResult = await client.query<{
    database_sessions: number;
    active_sessions: number;
    lock_waiters: number;
    hyperdrive_origin_sessions: number;
    temp_files: string;
    temp_bytes: string;
    deadlocks: string;
    cache_hit_percent: string | null;
  }>(`
    select
      (select count(*)::integer from pg_stat_activity where datname = current_database()) as database_sessions,
      (select count(*)::integer from pg_stat_activity where datname = current_database() and state = 'active') as active_sessions,
      (select count(*)::integer from pg_stat_activity where datname = current_database() and wait_event_type = 'Lock') as lock_waiters,
      (select count(*)::integer from pg_stat_activity where datname = current_database() and application_name = 'Cloudflare Hyperdrive') as hyperdrive_origin_sessions,
      d.temp_files::text as temp_files,
      d.temp_bytes::text as temp_bytes,
      d.deadlocks::text as deadlocks,
      case when d.blks_hit + d.blks_read = 0 then null
        else round(100.0 * d.blks_hit / (d.blks_hit + d.blks_read), 2)::text
      end as cache_hit_percent
    from pg_stat_database d
    where d.datname = current_database()
  `);
  const role = roleResult.rows[0];
  const sessions = sessionResult.rows[0];
  return benchmarkResponse({
    environment: "TEST",
    hyperdrive_binding: "HYPERDRIVE",
    cache_mode: "disabled",
    placement_mode: "targeted",
    database_is_expected_test: role?.expected_database === true,
    postgres_role_class: role?.owns_timesheets ? "Miget service owner" : "non-owner",
    postgres_role: {
      superuser: role?.superuser ?? null,
      bypass_rls: role?.bypass_rls ?? null,
      owns_timesheets: role?.owns_timesheets ?? null,
      row_security: role?.row_security ?? null,
      can_select_timesheets: role?.can_select_timesheets ?? null,
    },
    postgrest_jwt_role: decodeJwtRole(request.headers.get(POSTGREST_TOKEN_HEADER)?.trim() ?? ""),
    sessions,
  });
}

export async function handleDbBenchmark(request: Request, env: DbBenchmarkEnv): Promise<Response> {
  const handlerStartedAt = performance.now();
  if (!authorized(request, env)) {
    return benchmarkResponse({ error: "CloudTMS benchmark authorization required" }, 401);
  }
  if (request.method !== "GET") {
    return benchmarkResponse({ error: "Only GET is supported" }, 405);
  }

  const url = new URL(request.url);
  const action = url.pathname.slice(DB_BENCHMARK_PREFIX.length).replace(/^\//, "");

  try {
    if (action === "metadata") return await benchmarkMetadata(request, env);
    const test = requestedTest(url);
    const timesheetId = requestedTimesheetId(url, test);

    if (action === "postgrest") {
      const result = await runPostgrest(env, postgrestToken(request), test, timesheetId);
      result.handler_elapsed_ms = milliseconds(handlerStartedAt);
      return benchmarkResponse(result);
    }
    if (action === "hyperdrive") {
      const result = await runHyperdrive(env, test, requestedVariant(url), timesheetId);
      result.handler_elapsed_ms = milliseconds(handlerStartedAt);
      return benchmarkResponse(result);
    }
    if (action === "compare") {
      const variant = requestedVariant(url);
      const token = postgrestToken(request);
      const order = url.searchParams.get("order") === "hyperdrive_first"
        ? "hyperdrive_first"
        : "postgrest_first";
      let postgrest: PathMeasurement;
      let hyperdrive: PathMeasurement;
      if (order === "hyperdrive_first") {
        hyperdrive = await runHyperdrive(env, test, variant, timesheetId);
        postgrest = await runPostgrest(env, token, test, timesheetId);
      } else {
        postgrest = await runPostgrest(env, token, test, timesheetId);
        hyperdrive = await runHyperdrive(env, test, variant, timesheetId);
      }
      return benchmarkResponse({
        environment: "TEST",
        test,
        order,
        postgrest,
        hyperdrive,
        semantic_shape_equivalent: postgrest.shape_hash === hyperdrive.shape_hash,
        row_count_equivalent: postgrest.row_count === hyperdrive.row_count,
        result_byte_difference: hyperdrive.result_bytes - postgrest.result_bytes,
        handler_elapsed_ms: milliseconds(handlerStartedAt),
      });
    }
    return benchmarkResponse({ error: "Benchmark endpoint not found" }, 404);
  } catch (error) {
    const status = error instanceof BenchmarkHttpError ? error.status : 500;
    if (!(error instanceof BenchmarkHttpError)) {
      console.error(JSON.stringify({
        message: "TEST database benchmark failed",
        action,
        error: error instanceof Error ? error.message : "Unknown error",
      }));
    }
    return benchmarkResponse(
      {
        environment: "TEST",
        error: error instanceof BenchmarkHttpError ? error.message : "Benchmark execution failed",
      },
      status,
    );
  }
}
