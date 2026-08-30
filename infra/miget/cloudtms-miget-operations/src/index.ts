import { McpServer } from "@modelcontextprotocol/server";
import { createMcpHandler } from "agents/mcp/server";
import { Client } from "pg";
import { timingSafeEqual } from "node:crypto";
import { z } from "zod";
import {
  handleDbBenchmark,
  isDbBenchmarkPath,
  type DbBenchmarkEnv,
} from "./db-benchmark";

interface Env extends DbBenchmarkEnv {
  HYPERDRIVE: Hyperdrive;
  MYTMS_HYPERDRIVE: Hyperdrive;
  LIVE_HYPERDRIVE: Hyperdrive;
  MIGET_API_TOKEN: string;
  MIGET_MCP_ROUTE_TOKEN: string;
  MIGET_POSTGRES_SERVICE_ID: string;
  MYTMS_MIGET_POSTGRES_SERVICE_ID: string;
  LIVE_MIGET_POSTGRES_SERVICE_ID: string;
  MIGET_POSTGREST_ORIGIN: string;
}

const MIGET_API_BASE = "https://app.miget.com/api/v1";
const POSTGREST_PREFIX = "/rest/v1";
const MAX_RESPONSE_BYTES = 1_048_576;
const DATABASE_TARGET = z.enum(["agency_test", "mytms_test", "agency_live"]);
type DatabaseTarget = z.infer<typeof DATABASE_TARGET>;

const READ_ONLY_ANNOTATIONS = {
  readOnlyHint: true,
  destructiveHint: false,
  idempotentHint: true,
  openWorldHint: true,
} as const;

const SAFE_KEYS = new Set([
  "uuid",
  "id",
  "label",
  "name",
  "state",
  "status",
  "type",
  "service_type",
  "region",
  "created_at",
  "updated_at",
  "cpu_size",
  "ram_size",
  "disk_size",
  "instances",
  "postgres_version",
  "public_access",
  "project_id",
  "resource_id",
]);

const SAFE_PLAN_KEYS = new Set([
  "code_name",
  "name",
  "plan_type",
  "cpu_size",
  "ram_size",
  "disk_size",
  "unit_price",
]);

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function routeAuthorized(request: Request, env: Env): boolean {
  const expected = String(env.MIGET_MCP_ROUTE_TOKEN || "");
  const url = new URL(request.url);
  const bearer = request.headers.get("authorization")?.match(/^Bearer\s+(.+)$/i)?.[1] ?? "";
  const supplied = bearer || url.searchParams.get("access_token") || "";
  if (!expected || supplied.length !== expected.length) return false;
  return timingSafeEqual(Buffer.from(supplied), Buffer.from(expected));
}

function unwrap(payload: unknown): unknown {
  if (!isRecord(payload)) return payload;
  for (const key of ["data", "items", "results"]) {
    if (key in payload) return payload[key];
  }
  return payload;
}

function safeRecord(value: unknown): Record<string, unknown> {
  if (!isRecord(value)) return {};
  const output: Record<string, unknown> = {};
  for (const [key, child] of Object.entries(value)) {
    if (SAFE_KEYS.has(key) && ["string", "number", "boolean"].includes(typeof child)) {
      output[key] = child;
    }
    if (key === "plan" && isRecord(child)) {
      output.plan = Object.fromEntries(
        Object.entries(child).filter(
          ([planKey, planValue]) =>
            SAFE_PLAN_KEYS.has(planKey) &&
            ["string", "number", "boolean"].includes(typeof planValue),
        ),
      );
    }
  }
  return output;
}

function safeCollection(payload: unknown): Array<Record<string, unknown>> {
  const value = unwrap(payload);
  if (Array.isArray(value)) return value.map(safeRecord);
  return isRecord(value) ? [safeRecord(value)] : [];
}

async function migetGet(env: Env, path: string): Promise<unknown> {
  const response = await fetch(`${MIGET_API_BASE}${path}`, {
    method: "GET",
    headers: {
      Accept: "application/json",
      Authorization: `Bearer ${env.MIGET_API_TOKEN}`,
      "User-Agent": "cloudtms-chatgpt-miget-poc/1.0",
    },
  });
  if (!response.ok) {
    await response.body?.cancel();
    throw new Error(`Miget returned HTTP ${response.status}`);
  }
  const body = await response.arrayBuffer();
  if (body.byteLength > MAX_RESPONSE_BYTES) {
    throw new Error("Miget response exceeded the POC safety limit");
  }
  return JSON.parse(new TextDecoder().decode(body)) as unknown;
}

function resultContent(result: Record<string, unknown>) {
  return {
    content: [{ type: "text" as const, text: JSON.stringify(result, null, 2) }],
    structuredContent: result,
  };
}

async function withReadOnlyDatabase<T>(
  env: Env,
  database: DatabaseTarget,
  query: (client: Client) => Promise<T>,
): Promise<T> {
  const hyperdrive = database === "mytms_test"
    ? env.MYTMS_HYPERDRIVE
    : database === "agency_live"
      ? env.LIVE_HYPERDRIVE
      : env.HYPERDRIVE;
  const client = new Client({
    connectionString: hyperdrive.connectionString,
    application_name: `cloudtms-chatgpt-miget-${database}`,
    connectionTimeoutMillis: 10_000,
    query_timeout: 10_000,
  });
  await client.connect();
  try {
    await client.query("begin read only");
    await client.query("set local statement_timeout = '8s'");
    await client.query("set local lock_timeout = '1s'");
    await client.query("set local idle_in_transaction_session_timeout = '8s'");
    const value = await query(client);
    await client.query("commit");
    return value;
  } catch (error) {
    await client.query("rollback").catch(() => undefined);
    throw error;
  } finally {
    await client.end().catch(() => undefined);
  }
}

function createServer(env: Env): McpServer {
  const server = new McpServer({
    name: "CloudTMS Miget Read-Only Operations",
    version: "1.0.0",
  });

  server.registerTool(
    "miget_list_infrastructure",
    {
      title: "List Miget infrastructure",
      description:
        "Read-only. Authenticates to the disposable CloudTMS Miget project and lists its projects, compute resources, and services. Secret and connection fields are always removed.",
      inputSchema: z.object({}),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async () => {
      const [projects, resources, services] = await Promise.all([
        migetGet(env, "/projects"),
        migetGet(env, "/resources"),
        migetGet(env, "/services"),
      ]);
      return resultContent({
        authenticated: true,
        scope: "cloudtms-miget-estate",
        projects: safeCollection(projects),
        resources: safeCollection(resources),
        services: safeCollection(services),
      });
    },
  );

  server.registerTool(
    "miget_inspect_postgres",
    {
      title: "Inspect a CloudTMS Miget PostgreSQL service",
      description:
        "Read-only. Inspects the CloudTMS agency TEST, MyTMS TEST control-plane, or CloudTMS agency LIVE PostgreSQL service. Passwords, connection strings, and other secret fields are never returned.",
      inputSchema: z.object({
        database: DATABASE_TARGET.default("agency_test"),
      }),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async ({ database }) => {
      const serviceId = database === "mytms_test"
        ? env.MYTMS_MIGET_POSTGRES_SERVICE_ID
        : database === "agency_live"
          ? env.LIVE_MIGET_POSTGRES_SERVICE_ID
          : env.MIGET_POSTGRES_SERVICE_ID;
      const service = await migetGet(env, `/services/${serviceId}`);
      const records = safeCollection(service);
      return resultContent({
        authenticated: true,
        database,
        service: records[0] ?? {},
      });
    },
  );

  server.registerTool(
    "miget_verify_codex_parity_route",
    {
      title: "Verify the ChatGPT-to-Miget route",
      description:
        "Read-only acceptance probe. Proves that this ChatGPT session can invoke the permanent CloudTMS remote MCP, authenticate to Miget, and receive current infrastructure state without exposing credentials.",
      inputSchema: z.object({}),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async () => {
      const resources = safeCollection(await migetGet(env, "/resources"));
      return resultContent({
        accepted: resources.length > 0,
        authenticated: true,
        route: "ChatGPT web -> read-only MCP -> Miget REST API",
        resource_count: resources.length,
        secrets_returned: false,
      });
    },
  );

  server.registerTool(
    "miget_db_catalog_summary",
    {
      title: "Summarize a CloudTMS Miget PostgreSQL catalog",
      description:
        "Read-only PostgreSQL catalog inspection for agency_test, mytms_test, or agency_live. Returns database version and counts of non-system functions, triggers, RLS policies, relations, and installed extensions. It cannot execute caller-supplied SQL.",
      inputSchema: z.object({
        database: DATABASE_TARGET.default("agency_test"),
      }),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async ({ database }) => {
      const summary = await withReadOnlyDatabase(env, database, async (client) => {
        const response = await client.query(`
          select
            current_setting('server_version') as server_version,
            (
              select count(*)::integer
              from pg_proc p
              join pg_namespace n on n.oid = p.pronamespace
              where n.nspname not in ('pg_catalog', 'information_schema')
                and n.nspname !~ '^pg_toast'
            ) as function_count,
            (
              select count(*)::integer
              from pg_trigger t
              join pg_class c on c.oid = t.tgrelid
              join pg_namespace n on n.oid = c.relnamespace
              where not t.tgisinternal
                and n.nspname not in ('pg_catalog', 'information_schema')
            ) as trigger_count,
            (select count(*)::integer from pg_policy) as policy_count,
            (
              select count(*)::integer
              from pg_class c
              join pg_namespace n on n.oid = c.relnamespace
              where c.relkind in ('r', 'p', 'v', 'm', 'f')
                and n.nspname not in ('pg_catalog', 'information_schema')
                and n.nspname !~ '^pg_toast'
            ) as relation_count,
            (select count(*)::integer from pg_extension) as extension_count
        `);
        return response.rows[0] as Record<string, unknown>;
      });
      return resultContent({ authenticated: true, read_only: true, database, summary });
    },
  );

  server.registerTool(
    "miget_db_list_rpcs",
    {
      title: "List and search CloudTMS Miget PostgreSQL RPCs",
      description:
        "Read-only PostgreSQL catalog inspection for agency_test, mytms_test, or agency_live. Lists or searches non-system functions and procedures with signatures, result types, languages, volatility, and SECURITY DEFINER status. Results are paginated; no caller-supplied SQL is accepted.",
      inputSchema: z.object({
        database: DATABASE_TARGET.default("agency_test"),
        schema: z.string().trim().min(1).max(63).optional(),
        search: z.string().trim().min(1).max(128).optional(),
        limit: z.number().int().min(1).max(200).default(100),
        offset: z.number().int().min(0).max(100_000).default(0),
      }),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async ({ database, schema, search, limit, offset }) => {
      const catalog = await withReadOnlyDatabase(env, database, async (client) => {
        const values = [schema ?? null, search ? `%${search}%` : null, limit, offset];
        const totalResponse = await client.query(
          `
            select count(*)::integer as total
            from pg_proc p
            join pg_namespace n on n.oid = p.pronamespace
            where n.nspname not in ('pg_catalog', 'information_schema')
              and n.nspname !~ '^pg_toast'
              and ($1::text is null or n.nspname = $1)
              and ($2::text is null or p.proname ilike $2)
          `,
          values.slice(0, 2),
        );
        const rowsResponse = await client.query(
          `
            select
              n.nspname as schema_name,
              p.proname as function_name,
              pg_get_function_identity_arguments(p.oid) as identity_arguments,
              pg_get_function_result(p.oid) as result_type,
              l.lanname as language,
              case p.prokind when 'p' then 'procedure' else 'function' end as kind,
              p.prosecdef as security_definer,
              case p.provolatile when 'i' then 'immutable' when 's' then 'stable' else 'volatile' end as volatility
            from pg_proc p
            join pg_namespace n on n.oid = p.pronamespace
            join pg_language l on l.oid = p.prolang
            where n.nspname not in ('pg_catalog', 'information_schema')
              and n.nspname !~ '^pg_toast'
              and ($1::text is null or n.nspname = $1)
              and ($2::text is null or p.proname ilike $2)
            order by n.nspname, p.proname, pg_get_function_identity_arguments(p.oid)
            limit $3 offset $4
          `,
          values,
        );
        return {
          total: totalResponse.rows[0]?.total ?? 0,
          limit,
          offset,
          functions: rowsResponse.rows,
        };
      });
      return resultContent({ authenticated: true, read_only: true, database, ...catalog });
    },
  );

  server.registerTool(
    "miget_db_get_rpc_definition",
    {
      title: "Get a CloudTMS Miget PostgreSQL RPC definition",
      description:
        "Read-only PostgreSQL catalog inspection for agency_test, mytms_test, or agency_live. Returns exact pg_get_functiondef output for a named non-system function or procedure, including overload signatures. It cannot execute the function or caller-supplied SQL.",
      inputSchema: z.object({
        database: DATABASE_TARGET.default("agency_test"),
        schema: z.string().trim().min(1).max(63),
        name: z.string().trim().min(1).max(63),
        identity_arguments: z.string().max(2000).optional(),
      }),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async ({ database, schema, name, identity_arguments }) => {
      const definitions = await withReadOnlyDatabase(env, database, async (client) => {
        const response = await client.query(
          `
            select
              n.nspname as schema_name,
              p.proname as function_name,
              pg_get_function_identity_arguments(p.oid) as identity_arguments,
              pg_get_functiondef(p.oid) as definition
            from pg_proc p
            join pg_namespace n on n.oid = p.pronamespace
            where n.nspname = $1
              and p.proname = $2
              and ($3::text is null or pg_get_function_identity_arguments(p.oid) = $3)
              and n.nspname not in ('pg_catalog', 'information_schema')
            order by pg_get_function_identity_arguments(p.oid)
            limit 20
          `,
          [schema, name, identity_arguments ?? null],
        );
        return response.rows;
      });
      return resultContent({
        authenticated: true,
        read_only: true,
        database,
        matched: definitions.length,
        definitions,
      });
    },
  );

  server.registerTool(
    "miget_db_release_ledger",
    {
      title: "Inspect CloudTMS database release ledgers",
      description:
        "Read-only inspection of the protected migration and repeatable ledgers for agency_test, mytms_test, or agency_live. Returns counts and the most recent installed paths/hashes without accepting caller-supplied SQL.",
      inputSchema: z.object({
        database: DATABASE_TARGET.default("agency_test"),
        limit: z.number().int().min(1).max(100).default(25),
      }),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async ({ database, limit }) => {
      const ledger = await withReadOnlyDatabase(env, database, async (client) => {
        if (database === "mytms_test") {
          const availability = await client.query(`
            select
              to_regclass('public.schema_migrations') is not null as migration_ledger_present,
              to_regclass('public.schema_repeatables') is not null as repeatable_ledger_present
          `);
          const present = availability.rows[0] as Record<string, boolean>;
          if (!present.migration_ledger_present || !present.repeatable_ledger_present) {
            return {
              ledger_kind: "mytms_control_plane",
              ...present,
              migrations: [],
              repeatables: [],
            };
          }
          const [migrationCount, repeatableCount, migrations, repeatables] = await Promise.all([
            client.query("select count(*)::integer as count from public.schema_migrations"),
            client.query("select count(*)::integer as count from public.schema_repeatables"),
            client.query(
              `select filename as path, content_sha256 as sha256, applied_at
               from public.schema_migrations
               order by applied_at desc, filename desc limit $1`,
              [limit],
            ),
            client.query(
              `select filename as path, content_sha256 as sha256, applied_at
               from public.schema_repeatables
               order by applied_at desc, filename desc limit $1`,
              [limit],
            ),
          ]);
          return {
            ledger_kind: "mytms_control_plane",
            ...present,
            migration_count: migrationCount.rows[0]?.count ?? 0,
            repeatable_count: repeatableCount.rows[0]?.count ?? 0,
            migrations: migrations.rows,
            repeatables: repeatables.rows,
          };
        }
        const availability = await client.query(`
          select
            to_regclass('private.cloudtms_migration_ledger') is not null as migration_ledger_present,
            to_regclass('private.cloudtms_repeatable_ledger') is not null as repeatable_ledger_present
        `);
        const present = availability.rows[0] as Record<string, boolean>;
        if (!present.migration_ledger_present || !present.repeatable_ledger_present) {
          return {
            ledger_kind: "cloudtms_agency_release",
            ...present,
            migrations: [],
            repeatables: [],
          };
        }
        const [migrationCount, repeatableCount, migrations, repeatables] = await Promise.all([
          client.query("select count(*)::integer as count from private.cloudtms_migration_ledger"),
          client.query("select count(*)::integer as count from private.cloudtms_repeatable_ledger"),
          client.query(
            `select path, content_sha256 as sha256, first_release_id as release_id, applied_at_utc
             from private.cloudtms_migration_ledger
             order by applied_at_utc desc, path desc limit $1`,
            [limit],
          ),
          client.query(
            `select path, closure_sha256 as sha256, last_release_id as release_id, applied_at_utc
             from private.cloudtms_repeatable_ledger
             order by applied_at_utc desc, path desc limit $1`,
            [limit],
          ),
        ]);
        return {
          ledger_kind: "cloudtms_agency_release",
          ...present,
          migration_count: migrationCount.rows[0]?.count ?? 0,
          repeatable_count: repeatableCount.rows[0]?.count ?? 0,
          migrations: migrations.rows,
          repeatables: repeatables.rows,
        };
      });
      return resultContent({ authenticated: true, read_only: true, database, ledger });
    },
  );

  server.registerTool(
    "miget_db_security_audit",
    {
      title: "Audit CloudTMS PostgreSQL security posture",
      description:
        "Read-only fixed catalog audit for RLS coverage, exposed grants, PUBLIC function execution, SECURITY DEFINER search_path configuration, and installed extensions. It accepts no caller-supplied SQL and returns at most 200 findings per category.",
      inputSchema: z.object({
        database: DATABASE_TARGET.default("agency_test"),
      }),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async ({ database }) => {
      const audit = await withReadOnlyDatabase(env, database, async (client) => {
        const response = await client.query(`
          select
            (
              select jsonb_build_object(
                'table_count', count(*)::integer,
                'rls_enabled_count', count(*) filter (where c.relrowsecurity)::integer,
                'rls_forced_count', count(*) filter (where c.relforcerowsecurity)::integer
              )
              from pg_class c
              join pg_namespace n on n.oid = c.relnamespace
              where c.relkind in ('r','p')
                and n.nspname not in ('pg_catalog','information_schema')
                and n.nspname !~ '^pg_toast'
            ) as rls_summary,
            (
              select coalesce(jsonb_agg(to_jsonb(g) order by g.table_schema, g.table_name, g.grantee, g.privilege_type), '[]'::jsonb)
              from (
                select table_schema, table_name, grantee, privilege_type
                from information_schema.role_table_grants
                where grantee in ('PUBLIC','anon','authenticated','service_role','postgrest_authenticator')
                  and table_schema not in ('pg_catalog','information_schema')
                order by table_schema, table_name, grantee, privilege_type
                limit 200
              ) g
            ) as exposed_table_grants,
            (
              select coalesce(jsonb_agg(to_jsonb(f) order by f.schema_name, f.function_name, f.identity_arguments), '[]'::jsonb)
              from (
                select n.nspname as schema_name, p.proname as function_name,
                  pg_get_function_identity_arguments(p.oid) as identity_arguments,
                  p.prosecdef as security_definer,
                  exists (
                    select 1
                    from aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) acl
                    where acl.grantee = 0 and acl.privilege_type = 'EXECUTE'
                  ) as public_execute,
                  coalesce(array_to_string(p.proconfig, ','), '') as configuration
                from pg_proc p
                join pg_namespace n on n.oid = p.pronamespace
                where n.nspname not in ('pg_catalog','information_schema')
                  and n.nspname !~ '^pg_toast'
                  and (
                    exists (
                      select 1
                      from aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) acl
                      where acl.grantee = 0 and acl.privilege_type = 'EXECUTE'
                    )
                    or (
                      p.prosecdef
                      and not exists (
                        select 1 from unnest(coalesce(p.proconfig, array[]::text[])) setting
                        where setting like 'search_path=%'
                      )
                    )
                  )
                order by n.nspname, p.proname, pg_get_function_identity_arguments(p.oid)
                limit 200
              ) f
            ) as function_findings,
            (
              select coalesce(jsonb_agg(jsonb_build_object('name', e.extname, 'version', e.extversion) order by e.extname), '[]'::jsonb)
              from pg_extension e
            ) as extensions
        `);
        return response.rows[0] as Record<string, unknown>;
      });
      return resultContent({ authenticated: true, read_only: true, database, audit });
    },
  );

  server.registerTool(
    "miget_db_performance_summary",
    {
      title: "Summarize CloudTMS PostgreSQL performance",
      description:
        "Read-only fixed diagnostics for database size, sessions, active and blocked work, oldest transaction age, cache-hit ratio, temporary-file churn, deadlocks, and pg_stat_statements availability. Query text and sensitive payloads are never returned.",
      inputSchema: z.object({
        database: DATABASE_TARGET.default("agency_test"),
      }),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async ({ database }) => {
      const performance = await withReadOnlyDatabase(env, database, async (client) => {
        const response = await client.query(`
          select
            current_database() as database_name,
            pg_database_size(current_database())::bigint as database_bytes,
            d.numbackends::integer as database_connections,
            d.xact_commit::bigint as transactions_committed,
            d.xact_rollback::bigint as transactions_rolled_back,
            d.temp_files::bigint as temp_files,
            d.temp_bytes::bigint as temp_bytes,
            d.deadlocks::bigint as deadlocks,
            case when d.blks_hit + d.blks_read = 0 then null
              else round(100.0 * d.blks_hit / (d.blks_hit + d.blks_read), 2)
            end as cache_hit_percent,
            (select count(*)::integer from pg_stat_activity where datname = current_database()) as sessions,
            (select count(*)::integer from pg_stat_activity where datname = current_database() and state = 'active') as active_sessions,
            (select count(*)::integer from pg_stat_activity where datname = current_database() and wait_event_type = 'Lock') as lock_waiters,
            (select count(*)::integer from pg_locks where not granted) as ungranted_locks,
            coalesce((
              select extract(epoch from (clock_timestamp() - min(xact_start)))::bigint
              from pg_stat_activity where datname = current_database() and xact_start is not null
            ), 0) as oldest_transaction_seconds,
            exists(select 1 from pg_extension where extname = 'pg_stat_statements') as pg_stat_statements_installed
          from pg_stat_database d
          where d.datname = current_database()
        `);
        return response.rows[0] as Record<string, unknown>;
      });
      return resultContent({ authenticated: true, read_only: true, database, performance });
    },
  );

  return server;
}

async function proxyPostgrest(request: Request, env: Env): Promise<Response> {
  const inbound = new URL(request.url);
  const upstream = new URL(env.MIGET_POSTGREST_ORIGIN);
  const suffix = inbound.pathname.slice(POSTGREST_PREFIX.length) || "/";
  upstream.pathname = `${upstream.pathname.replace(/\/$/, "")}${
    suffix.startsWith("/") ? suffix : `/${suffix}`
  }`;
  upstream.search = inbound.search;

  const headers = new Headers(request.headers);
  for (const name of [
    "host",
    "cf-connecting-ip",
    "cf-ipcountry",
    "cf-ray",
    "cf-visitor",
    "x-forwarded-for",
    "x-forwarded-proto",
  ]) {
    headers.delete(name);
  }

  const init: RequestInit = {
    method: request.method,
    headers,
    redirect: "manual",
  };
  if (request.method !== "GET" && request.method !== "HEAD") {
    init.body = request.body;
  }

  const response = await fetch(upstream, init);
  console.log(JSON.stringify({
    message: "PostgREST gateway diagnostic",
    method: request.method,
    path: inbound.pathname,
    authorization_present: headers.has("authorization"),
    apikey_present: headers.has("apikey"),
    upstream_status: response.status,
  }));
  const responseHeaders = new Headers(response.headers);
  responseHeaders.set("x-cloudtms-miget-gateway", "poc-v1");
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers: responseHeaders,
  });
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    const route = "/mcp";
    if (isDbBenchmarkPath(url.pathname)) {
      return await handleDbBenchmark(request, env);
    }
    if (url.pathname === "/miget-gateway/health") {
      return Response.json({
        ok: true,
        service: "cloudtms-miget-gateway",
        version: "1.0.0",
        databases: ["agency_test", "mytms_test", "agency_live"],
      });
    }
    if (url.pathname === POSTGREST_PREFIX || url.pathname.startsWith(`${POSTGREST_PREFIX}/`)) {
      try {
        return await proxyPostgrest(request, env);
      } catch (error) {
        console.error(JSON.stringify({
          message: "PostgREST gateway request failed",
          error: error instanceof Error ? error.message : "Unknown error",
        }));
        return Response.json({ error: "PostgREST gateway failed" }, { status: 502 });
      }
    }
    if (url.pathname !== route) {
      return new Response("Not found", { status: 404 });
    }
    if (!routeAuthorized(request, env)) {
      return Response.json(
        { error: "CloudTMS MCP authorization required" },
        { status: 401, headers: { "cache-control": "no-store" } },
      );
    }

    try {
      const handler = createMcpHandler(() => createServer(env), {
        route,
        legacy: "stateless",
        responseMode: "auto",
        onerror(error) {
          console.error(
            JSON.stringify({
              message: "MCP request failed",
              error: error.message,
            }),
          );
        },
      });
      return await handler(request, env, ctx);
    } catch (error) {
      console.error(
        JSON.stringify({
          message: "MCP bridge failed",
          error: error instanceof Error ? error.message : "Unknown error",
        }),
      );
      return Response.json({ error: "MCP bridge failed" }, { status: 500 });
    }
  },
} satisfies ExportedHandler<Env>;
