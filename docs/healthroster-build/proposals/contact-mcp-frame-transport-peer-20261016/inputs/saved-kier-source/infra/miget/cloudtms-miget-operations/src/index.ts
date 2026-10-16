import { McpServer } from "@modelcontextprotocol/server";
import { createMcpHandler } from "agents/mcp/server";
import { Client } from "pg";
import { timingSafeEqual } from "node:crypto";
import { z } from "zod";
import { runRollbackTransaction } from "./rollback-transaction";

interface Env {
  HYPERDRIVE: Hyperdrive;
  MYTMS_HYPERDRIVE: Hyperdrive;
  LIVE_HYPERDRIVE?: Hyperdrive;
  MIGET_API_TOKEN: string;
  MIGET_MCP_ROUTE_TOKEN: string;
  MIGET_CODEX_BEARER_TOKEN?: string;
  MIGET_CHATGPT_WRAPPER_TOKEN?: string;
  MIGET_POSTGRES_SERVICE_ID: string;
  MYTMS_MIGET_POSTGRES_SERVICE_ID: string;
  LIVE_MIGET_POSTGRES_SERVICE_ID?: string;
  MIGET_TEST_ONLY?: string;
  MIGET_TRANSACTION_ROLLBACK_ENABLED?: string;
  MIGET_POSTGREST_ORIGIN: string;
  MIGET_RESOURCE_ID?: string;
  MIGET_WORKSPACE_ID?: string;
}

const MIGET_API_BASE = "https://app.miget.com/api/v1";
const MIGET_METRICS_BASE = "https://metrics.miget.com/prometheus/api/v1";
const POSTGREST_PREFIX = "/rest/v1";
const MAX_RESPONSE_BYTES = 1_048_576;
const DATABASE_TARGET = z.enum(["agency_test", "mytms_test", "agency_live"]);
type DatabaseTarget = z.infer<typeof DATABASE_TARGET>;
const TEST_DATABASE_TARGET = z.enum(["agency_test", "mytms_test"]);
const DATABASE_CODE_KIND = z.enum([
  "function",
  "procedure",
  "window_function",
  "view",
  "materialized_view",
  "rule",
  "trigger",
  "policy",
  "index",
  "constraint",
  "column_expression",
  "event_trigger",
]);
const DATABASE_CODE_SEARCH_TERMS = z.array(z.string().trim().min(1).max(128)).min(1).max(8)
  .superRefine((terms, context) => {
    const seen = new Set<string>();
    for (const [index, term] of terms.entries()) {
      const normalized = term.toLocaleLowerCase("en-US");
      if (seen.has(normalized)) {
        context.addIssue({ code: z.ZodIssueCode.custom, message: "Search terms must be unique", path: [index] });
      }
      seen.add(normalized);
    }
  });
const DATABASE_CODE_SEARCH_SCHEMAS = z.array(z.string().trim().min(1).max(63)).min(1).max(20)
  .superRefine((schemas, context) => {
    const seen = new Set<string>();
    for (const [index, schema] of schemas.entries()) {
      if (seen.has(schema)) {
        context.addIssue({ code: z.ZodIssueCode.custom, message: "Schema names must be unique", path: [index] });
      }
      seen.add(schema);
    }
  });
const DATABASE_CODE_SEARCH_KINDS = z.array(DATABASE_CODE_KIND).min(1).max(12)
  .superRefine((kinds, context) => {
    const seen = new Set<string>();
    for (const [index, kind] of kinds.entries()) {
      if (seen.has(kind)) {
        context.addIssue({ code: z.ZodIssueCode.custom, message: "Code kinds must be unique", path: [index] });
      }
      seen.add(kind);
    }
  });
const BANKING_PAY_R11_ROLES = [
  "anon",
  "authenticated",
  "service_role",
  "postgrest_authenticator",
] as const;
const BANKING_PAY_R11_OBJECTS = [
  ["public", "banking_pay_operation_candidate_allocation_rows"],
  ["public", "banking_pay_operation_candidate_scope"],
  ["private", "cloudtms_database_identity"],
  ["private", "cloudtms_database_releases"],
  ["private", "cloudtms_migration_ledger"],
  ["private", "cloudtms_repeatable_ledger"],
] as const;
const RELATION_METADATA_SECTION = z.enum([
  "relations",
  "columns",
  "triggers",
  "rules",
  "constraints",
  "foreign_keys",
  "dependencies",
  "privileges",
  "policies",
  "indexes",
]);
const RELATION_METADATA_DEFAULT_ROLES = [
  "anon",
  "authenticated",
  "service_role",
  "postgrest_authenticator",
] as const;
const RELATION_METADATA_TARGET = z.object({
  schema: z.string().trim().min(1).max(63),
  relation: z.string().trim().min(1).max(63),
}).strict();
const RELATION_METADATA_TARGETS = z.array(RELATION_METADATA_TARGET).min(1).max(50).superRefine((targets, context) => {
  const seen = new Set<string>();
  for (const [index, target] of targets.entries()) {
    const key = `${target.schema}\u0000${target.relation}`;
    if (seen.has(key)) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: "Relation targets must be unique",
        path: [index],
      });
    }
    seen.add(key);
  }
});
const RELATION_METADATA_ROLES = z.array(z.string().trim().min(1).max(63)).max(20).superRefine((roles, context) => {
  const seen = new Set<string>();
  for (const [index, role] of roles.entries()) {
    if (seen.has(role)) {
      context.addIssue({ code: z.ZodIssueCode.custom, message: "Role names must be unique", path: [index] });
    }
    seen.add(role);
  }
});
const RELATION_METADATA_INDEX_NAMES = z.array(z.string().trim().min(1).max(63)).min(1).max(50)
  .superRefine((indexNames, context) => {
    const seen = new Set<string>();
    for (const [index, indexName] of indexNames.entries()) {
      if (seen.has(indexName)) {
        context.addIssue({ code: z.ZodIssueCode.custom, message: "Index names must be unique", path: [index] });
      }
      seen.add(indexName);
    }
  });
const ROUTINE_METADATA_TARGET = z.object({
  schema: z.string().trim().min(1).max(63),
  name: z.string().trim().min(1).max(63),
  identity_arguments: z.string().max(2_000),
}).strict();
const ROUTINE_METADATA_TARGETS = z.array(ROUTINE_METADATA_TARGET).min(1).max(50).superRefine((targets, context) => {
  const seen = new Set<string>();
  for (const [index, target] of targets.entries()) {
    const key = `${target.schema}\u0000${target.name}\u0000${target.identity_arguments}`;
    if (seen.has(key)) {
      context.addIssue({ code: z.ZodIssueCode.custom, message: "Routine targets must be unique", path: [index] });
    }
    seen.add(key);
  }
});
const ROUTINE_METADATA_ROLES = z.array(z.string().trim().min(1).max(63)).max(20).superRefine((roles, context) => {
  const seen = new Set<string>();
  for (const [index, role] of roles.entries()) {
    if (seen.has(role)) {
      context.addIssue({ code: z.ZodIssueCode.custom, message: "Role names must be unique", path: [index] });
    }
    seen.add(role);
  }
});

const READ_ONLY_ANNOTATIONS = {
  readOnlyHint: true,
  destructiveHint: false,
  idempotentHint: true,
  openWorldHint: true,
} as const;

const ROLLBACK_ONLY_ANNOTATIONS = {
  readOnlyHint: false,
  destructiveHint: true,
  idempotentHint: false,
  openWorldHint: false,
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

const SERVICE_CAPACITY_KEYS = new Set([
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

const RESOURCE_CAPACITY_KEYS = new Set([
  "uuid",
  "id",
  "name",
  "state",
  "created_at",
  "updated_at",
  "total_disk_size",
  "total_ram_size",
  "total_cpu_size",
  "total_used_disk_size",
  "total_used_ram_size",
  "total_used_cpu_size",
  "available_disk_size",
  "available_ram_size",
  "available_cpu_size",
  "assigned",
]);

const METRIC_LABEL_KEYS = new Set([
  "namespace",
  "app",
  "addon",
  "addon_type",
  "instance",
  "database",
  "role",
  "volume",
  "device",
]);

const BANKING_PAY_OBSERVATION_METRICS = [
  "miget_volume_used_bytes",
  "miget_volume_size_bytes",
  "miget_addon_pg_database_size_bytes",
  "miget_addon_pg_up",
] as const;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function routeAuthorized(request: Request, env: Env): boolean {
  const url = new URL(request.url);
  const bearer = request.headers.get("authorization")?.match(/^Bearer\s+(.+)$/i)?.[1] ?? "";
  const supplied = bearer || url.searchParams.get("access_token") || "";
  const isChatGptWrapper = request.headers.get("x-cloudtms-internal-route") === "chatgpt-test-wrapper-v1";
  if (isChatGptWrapper) {
    const expected = String(env.MIGET_CHATGPT_WRAPPER_TOKEN || "");
    if (!bearer || !expected || bearer.length !== expected.length) return false;
    return timingSafeEqual(Buffer.from(bearer), Buffer.from(expected));
  }
  const expected = bearer
    ? String(env.MIGET_CODEX_BEARER_TOKEN || env.MIGET_MCP_ROUTE_TOKEN || "")
    : String(env.MIGET_MCP_ROUTE_TOKEN || "");
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

function safeScalarRecord(value: unknown, allowedKeys: ReadonlySet<string>): Record<string, unknown> {
  if (!isRecord(value)) return {};
  return Object.fromEntries(
    Object.entries(value).filter(
      ([key, child]) => allowedKeys.has(key) && ["string", "number", "boolean"].includes(typeof child),
    ),
  );
}

function unwrapRecord(payload: unknown): Record<string, unknown> {
  const value = unwrap(payload);
  if (isRecord(value)) return value;
  if (Array.isArray(value) && value.length === 1 && isRecord(value[0])) return value[0];
  return {};
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

async function migetMetricsQuery(
  env: Env,
  metricName: typeof BANKING_PAY_OBSERVATION_METRICS[number],
  targetIdentifiers: ReadonlySet<string>,
) {
  const url = new URL(`${MIGET_METRICS_BASE}/query`);
  url.searchParams.set("query", metricName);
  const headers = new Headers({
    Accept: "application/json",
    Authorization: `Bearer ${env.MIGET_API_TOKEN}`,
    "User-Agent": "cloudtms-chatgpt-miget-poc/1.0",
  });
  if (env.MIGET_WORKSPACE_ID) headers.set("X-Workspace-Id", env.MIGET_WORKSPACE_ID);
  const response = await fetch(url, { method: "GET", headers });
  if (!response.ok) {
    await response.body?.cancel();
    throw new Error(`Miget metrics returned HTTP ${response.status}`);
  }
  const body = await response.arrayBuffer();
  if (body.byteLength > MAX_RESPONSE_BYTES) {
    throw new Error("Miget metrics response exceeded the safety limit");
  }
  const payload = JSON.parse(new TextDecoder().decode(body)) as unknown;
  const data = isRecord(payload) && isRecord(payload.data) ? payload.data : {};
  const rawResults = Array.isArray(data.result) ? data.result : [];
  const workspaceResults = rawResults.slice(0, 100).flatMap((entry) => {
    if (!isRecord(entry)) return [];
    const labels = safeScalarRecord(entry.metric, METRIC_LABEL_KEYS);
    const value = Array.isArray(entry.value) && entry.value.length === 2
      ? { timestamp: entry.value[0], value: entry.value[1] }
      : null;
    return [{ labels, value }];
  });
  const results = workspaceResults.filter(({ labels }) => (
    [labels.app, labels.addon].some(
      (value) => typeof value === "string" && targetIdentifiers.has(value),
    )
  ));
  const observedIdentities = [...new Set(workspaceResults.flatMap(({ labels }) => (
    [labels.app, labels.addon].filter((value): value is string => typeof value === "string")
  )))].sort().slice(0, 50);
  return {
    metric: metricName,
    status: isRecord(payload) && typeof payload.status === "string" ? payload.status : "unknown",
    result_type: typeof data.resultType === "string" ? data.resultType : null,
    workspace_total_series: rawResults.length,
    workspace_result_truncated: rawResults.length > workspaceResults.length,
    observed_app_or_addon_identities: observedIdentities,
    target_total_series: results.length,
    target_series_found: results.length > 0,
    series: results,
  };
}

async function observeMigetMetric(
  env: Env,
  metricName: typeof BANKING_PAY_OBSERVATION_METRICS[number],
  targetIdentifiers: ReadonlySet<string>,
) {
  try {
    return await migetMetricsQuery(env, metricName, targetIdentifiers);
  } catch (error) {
    return {
      metric: metricName,
      status: "unavailable",
      error: error instanceof Error ? error.message : "Miget metrics request failed",
      workspace_total_series: null,
      workspace_result_truncated: false,
      observed_app_or_addon_identities: [],
      target_total_series: null,
      target_series_found: false,
      series: [],
    };
  }
}

async function collectCapacityDatabaseObservation(env: Env, database: DatabaseTarget) {
  return withReadOnlyDatabase(env, database, async (client) => {
    const response = await client.query(`
      select
        current_database() as database_name,
        current_user as connection_role,
        current_setting('server_version') as server_version,
        current_setting('server_version_num')::integer as server_version_num,
        pg_is_in_recovery() as in_recovery,
        pg_postmaster_start_time() as postmaster_started_at,
        pg_database_size(current_database())::bigint as database_bytes,
        (select count(*)::integer from pg_stat_replication) as connected_replica_count,
        (select count(*)::integer from pg_replication_slots where active) as active_replication_slot_count,
        (select count(*)::integer from pg_prepared_xacts) as prepared_transaction_count
    `);
    return response.rows[0] as Record<string, unknown>;
  });
}

function serviceIdForDatabase(env: Env, database: DatabaseTarget): string {
  if (env.MIGET_TEST_ONLY === "true" && database === "agency_live") {
    throw new Error("LIVE is unavailable on this TEST-only connector");
  }
  return database === "mytms_test"
    ? env.MYTMS_MIGET_POSTGRES_SERVICE_ID
    : database === "agency_live"
      ? env.LIVE_MIGET_POSTGRES_SERVICE_ID || ""
      : env.MIGET_POSTGRES_SERVICE_ID;
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
  if (env.MIGET_TEST_ONLY === "true" && database === "agency_live") {
    throw new Error("LIVE is unavailable on this TEST-only connector");
  }
  const hyperdrive = database === "mytms_test"
    ? env.MYTMS_HYPERDRIVE
    : database === "agency_live"
      ? env.LIVE_HYPERDRIVE
      : env.HYPERDRIVE;
  if (!hyperdrive) throw new Error(`No Hyperdrive is configured for ${database}`);
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

function quotePgIdentifier(value: string): string {
  return `"${value.replaceAll('"', '""')}"`;
}

function postgresErrorCode(error: unknown): string {
  return isRecord(error) && typeof error.code === "string" ? error.code : "UNKNOWN";
}

async function collectBankingPayR11RoleProbes(client: Client) {
  const probes: Array<Record<string, unknown>> = [];
  for (const roleName of BANKING_PAY_R11_ROLES) {
    const role = await client.query(
      `select oid, rolcanlogin, rolsuper, rolinherit, rolbypassrls
       from pg_roles where rolname = $1`,
      [roleName],
    );
    if (role.rowCount !== 1) {
      probes.push({ role: roleName, role_present: false, set_role_succeeded: false, objects: [] });
      continue;
    }

    await client.query("savepoint banking_pay_r11_role_probe");
    try {
      await client.query(`set local role ${quotePgIdentifier(roleName)}`);
      await client.query("set local row_security = on");
      const objectFacts: Array<Record<string, unknown>> = [];
      for (const [schemaName, objectName] of BANKING_PAY_R11_OBJECTS) {
        const qualifiedName = `${quotePgIdentifier(schemaName)}.${quotePgIdentifier(objectName)}`;
        const facts = await client.query(
          `select
             to_regclass($1) is not null as object_present,
             has_schema_privilege(current_user, $2, 'USAGE') as schema_usage,
             case when to_regclass($1) is null then null else has_table_privilege(current_user, to_regclass($1), 'SELECT') end as select_privilege,
             case when to_regclass($1) is null then null else has_table_privilege(current_user, to_regclass($1), 'INSERT') end as insert_privilege,
             case when to_regclass($1) is null then null else has_table_privilege(current_user, to_regclass($1), 'UPDATE') end as update_privilege,
             case when to_regclass($1) is null then null else has_table_privilege(current_user, to_regclass($1), 'DELETE') end as delete_privilege,
             case when to_regclass($1) is null then null else has_table_privilege(current_user, to_regclass($1), 'TRUNCATE') end as truncate_privilege,
             case when to_regclass($1) is null then null else row_security_active(to_regclass($1)) end as row_security_active`,
          [`${schemaName}.${objectName}`, schemaName],
        );
        let zeroRowSelectSucceeded = false;
        let zeroRowSelectSqlstate: string | null = null;
        if (facts.rows[0]?.object_present) {
          await client.query("savepoint banking_pay_r11_object_probe");
          try {
            await client.query(`select 1 from ${qualifiedName} where false limit 0`);
            zeroRowSelectSucceeded = true;
            await client.query("release savepoint banking_pay_r11_object_probe");
          } catch (error) {
            zeroRowSelectSqlstate = postgresErrorCode(error);
            await client.query("rollback to savepoint banking_pay_r11_object_probe");
            await client.query("release savepoint banking_pay_r11_object_probe");
          }
        }
        objectFacts.push({
          schema: schemaName,
          object: objectName,
          ...(facts.rows[0] as Record<string, unknown>),
          zero_row_select_succeeded: zeroRowSelectSucceeded,
          zero_row_select_sqlstate: zeroRowSelectSqlstate,
          rows_returned: 0,
        });
      }
      probes.push({
        role: roleName,
        role_present: true,
        set_role_succeeded: true,
        role_attributes: role.rows[0],
        objects: objectFacts,
      });
      await client.query("reset role");
      await client.query("release savepoint banking_pay_r11_role_probe");
    } catch (error) {
      await client.query("rollback to savepoint banking_pay_r11_role_probe");
      await client.query("release savepoint banking_pay_r11_role_probe");
      probes.push({
        role: roleName,
        role_present: true,
        set_role_succeeded: false,
        set_role_sqlstate: postgresErrorCode(error),
        role_attributes: role.rows[0],
        objects: [],
      });
    }
  }
  return probes;
}

async function collectBankingPayR11CatalogEvidence(client: Client) {
  const response = await client.query(`
    with recursive
    target_objects(schema_name, object_name, ordinal) as (
      values
        ('public', 'banking_pay_operation_candidate_allocation_rows', 1),
        ('public', 'banking_pay_operation_candidate_scope', 2),
        ('private', 'cloudtms_database_identity', 3),
        ('private', 'cloudtms_database_releases', 4),
        ('private', 'cloudtms_migration_ledger', 5),
        ('private', 'cloudtms_repeatable_ledger', 6)
    ),
    target_roles(role_name, ordinal) as (
      values ('anon', 1), ('authenticated', 2), ('service_role', 3), ('postgrest_authenticator', 4)
    ),
    objects as (
      select t.schema_name, t.object_name, t.ordinal,
        n.oid as schema_oid, c.oid as relation_oid, c.relowner,
        pg_get_userbyid(c.relowner) as owner,
        c.relkind, c.relrowsecurity, c.relforcerowsecurity, c.relacl,
        (select count(*)::integer from pg_attribute a
         where a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped) as column_count
      from target_objects t
      left join pg_namespace n on n.nspname = t.schema_name
      left join pg_class c on c.relnamespace = n.oid
        and c.relname = t.object_name and c.relkind in ('r','p','v','m','f')
    ),
    roles as (
      select t.role_name, t.ordinal, r.oid as role_oid,
        r.rolcanlogin, r.rolsuper, r.rolinherit, r.rolcreaterole,
        r.rolcreatedb, r.rolreplication, r.rolbypassrls
      from target_roles t left join pg_roles r on r.rolname = t.role_name
    ),
    membership_closure(start_oid, inherited_oid, depth, path) as (
      select r.role_oid, r.role_oid, 0, array[r.role_oid]
      from roles r where r.role_oid is not null
      union all
      select m.start_oid, a.roleid, m.depth + 1, m.path || a.roleid
      from membership_closure m
      join pg_auth_members a on a.member = m.inherited_oid
      where m.depth < 32 and not a.roleid = any(m.path)
    ),
    accessor_functions as (
      select distinct p.oid, n.nspname as schema_name, p.proname as function_name,
        pg_get_function_identity_arguments(p.oid) as identity_arguments,
        pg_get_userbyid(p.proowner) as owner, p.prosecdef as security_definer,
        coalesce(array_to_string(p.proconfig, ','), '') as configuration
      from pg_proc p
      join pg_namespace n on n.oid = p.pronamespace
      where p.prokind in ('f','p')
        and exists (
          select 1 from objects o
          where o.relation_oid is not null and strpos(lower(p.prosrc), lower(o.object_name)) > 0
        )
    )
    select
      current_database() as database_name,
      current_user as connection_role,
      session_user as session_role,
      current_setting('row_security', true) as connection_row_security,
      (select count(*)::integer from objects where relation_oid is not null) as resolved_object_count,
      (select count(*)::integer from roles where role_oid is not null) as resolved_role_count,
      (select coalesce(jsonb_agg(jsonb_build_object(
        'schema', o.schema_name, 'object', o.object_name, 'present', o.relation_oid is not null,
        'owner', o.owner, 'relation_kind', o.relkind, 'rls_enabled', o.relrowsecurity,
        'rls_forced', o.relforcerowsecurity, 'column_count', o.column_count,
        'policy_count', (select count(*)::integer from pg_policy p where p.polrelid = o.relation_oid)
      ) order by o.ordinal), '[]'::jsonb) from objects o) as objects,
      (select coalesce(jsonb_agg(jsonb_build_object(
        'role', r.role_name, 'present', r.role_oid is not null, 'can_login', r.rolcanlogin,
        'superuser', r.rolsuper, 'inherits', r.rolinherit, 'create_role', r.rolcreaterole,
        'create_database', r.rolcreatedb, 'replication', r.rolreplication, 'bypass_rls', r.rolbypassrls
      ) order by r.ordinal), '[]'::jsonb) from roles r) as roles,
      (select coalesce(jsonb_agg(jsonb_build_object(
        'member', pg_get_userbyid(a.member), 'granted_role', pg_get_userbyid(a.roleid),
        'member_can_login', member_role.rolcanlogin,
        'member_superuser', member_role.rolsuper,
        'member_bypass_rls', member_role.rolbypassrls,
        'member_inherits', member_role.rolinherit,
        'admin_option', a.admin_option, 'inherit_option', a.inherit_option, 'set_option', a.set_option
      ) order by pg_get_userbyid(a.member), pg_get_userbyid(a.roleid)), '[]'::jsonb)
       from pg_auth_members a
       join pg_roles member_role on member_role.oid = a.member
       where a.member in (select distinct inherited_oid from membership_closure)
          or a.roleid in (select role_oid from roles where role_oid is not null)) as role_memberships,
      (select coalesce(jsonb_agg(jsonb_build_object(
        'schema', s.nspname, 'owner', pg_get_userbyid(s.nspowner), 'role', r.role_name,
        'usage', case when r.role_oid is null then null else has_schema_privilege(r.role_oid, s.oid, 'USAGE') end,
        'create', case when r.role_oid is null then null else has_schema_privilege(r.role_oid, s.oid, 'CREATE') end
      ) order by s.nspname, r.ordinal), '[]'::jsonb)
       from pg_namespace s cross join roles r where s.nspname in ('public','private')) as schema_access,
      (select coalesce(jsonb_agg(jsonb_build_object(
        'schema', o.schema_name, 'object', o.object_name, 'role', r.role_name,
        'role_present', r.role_oid is not null, 'object_present', o.relation_oid is not null,
        'connection_role_is_member', case when r.role_oid is null then null else pg_has_role(current_user, r.role_oid, 'MEMBER') end,
        'owner_match', case when r.role_oid is null or o.relation_oid is null then null else r.role_oid = o.relowner end,
        'superuser', r.rolsuper, 'bypass_rls', r.rolbypassrls,
        'schema_usage', case when r.role_oid is null or o.schema_oid is null then null else has_schema_privilege(r.role_oid, o.schema_oid, 'USAGE') end,
        'select', case when r.role_oid is null or o.relation_oid is null then null else has_table_privilege(r.role_oid, o.relation_oid, 'SELECT') end,
        'insert', case when r.role_oid is null or o.relation_oid is null then null else has_table_privilege(r.role_oid, o.relation_oid, 'INSERT') end,
        'update', case when r.role_oid is null or o.relation_oid is null then null else has_table_privilege(r.role_oid, o.relation_oid, 'UPDATE') end,
        'delete', case when r.role_oid is null or o.relation_oid is null then null else has_table_privilege(r.role_oid, o.relation_oid, 'DELETE') end,
        'truncate', case when r.role_oid is null or o.relation_oid is null then null else has_table_privilege(r.role_oid, o.relation_oid, 'TRUNCATE') end,
        'references', case when r.role_oid is null or o.relation_oid is null then null else has_table_privilege(r.role_oid, o.relation_oid, 'REFERENCES') end,
        'trigger', case when r.role_oid is null or o.relation_oid is null then null else has_table_privilege(r.role_oid, o.relation_oid, 'TRIGGER') end,
        'select_column_count', case when r.role_oid is null or o.relation_oid is null then null else (
          select count(*)::integer from pg_attribute a where a.attrelid = o.relation_oid and a.attnum > 0 and not a.attisdropped
            and has_column_privilege(r.role_oid, o.relation_oid, a.attnum, 'SELECT')) end,
        'insert_column_count', case when r.role_oid is null or o.relation_oid is null then null else (
          select count(*)::integer from pg_attribute a where a.attrelid = o.relation_oid and a.attnum > 0 and not a.attisdropped
            and has_column_privilege(r.role_oid, o.relation_oid, a.attnum, 'INSERT')) end,
        'update_column_count', case when r.role_oid is null or o.relation_oid is null then null else (
          select count(*)::integer from pg_attribute a where a.attrelid = o.relation_oid and a.attnum > 0 and not a.attisdropped
            and has_column_privilege(r.role_oid, o.relation_oid, a.attnum, 'UPDATE')) end,
        'references_column_count', case when r.role_oid is null or o.relation_oid is null then null else (
          select count(*)::integer from pg_attribute a where a.attrelid = o.relation_oid and a.attnum > 0 and not a.attisdropped
            and has_column_privilege(r.role_oid, o.relation_oid, a.attnum, 'REFERENCES')) end,
        'rls_enabled', o.relrowsecurity, 'rls_forced', o.relforcerowsecurity,
        'rls_bypass_by_attribute_or_owner', case when r.role_oid is null or o.relation_oid is null then null
          else r.rolsuper or r.rolbypassrls or (r.role_oid = o.relowner and not o.relforcerowsecurity) end,
        'applicable_policy_count', case when r.role_oid is null or o.relation_oid is null then null else (
          select count(*)::integer from pg_policy p where p.polrelid = o.relation_oid and exists (
            select 1 from unnest(p.polroles) policy_role
            where policy_role = 0 or policy_role = r.role_oid or pg_has_role(r.role_oid, policy_role, 'MEMBER')
          )) end
      ) order by o.ordinal, r.ordinal), '[]'::jsonb) from objects o cross join roles r) as effective_object_access,
      (select coalesce(jsonb_agg(jsonb_build_object(
        'schema', o.schema_name, 'object', o.object_name, 'policy', p.polname,
        'command', p.polcmd, 'permissive', p.polpermissive,
        'roles', (select coalesce(jsonb_agg(case when policy_role = 0 then 'PUBLIC' else pg_get_userbyid(policy_role) end order by policy_role), '[]'::jsonb) from unnest(p.polroles) policy_role),
        'using_expression', pg_get_expr(p.polqual, p.polrelid), 'check_expression', pg_get_expr(p.polwithcheck, p.polrelid)
      ) order by o.ordinal, p.polname), '[]'::jsonb)
       from objects o join pg_policy p on p.polrelid = o.relation_oid) as policies,
      (select coalesce(jsonb_agg(jsonb_build_object(
        'schema', o.schema_name, 'object', o.object_name, 'grantor', pg_get_userbyid(a.grantor),
        'grantee', case when a.grantee = 0 then 'PUBLIC' else pg_get_userbyid(a.grantee) end,
        'privilege', a.privilege_type, 'grantable', a.is_grantable
      ) order by o.ordinal, a.grantee, a.privilege_type), '[]'::jsonb)
       from objects o cross join lateral aclexplode(coalesce(o.relacl, acldefault('r', o.relowner))) a
       where o.relation_oid is not null) as explicit_and_default_object_acl,
      (select coalesce(jsonb_agg(jsonb_build_object(
        'owner', pg_get_userbyid(d.defaclrole), 'schema', coalesce(n.nspname, 'ALL_SCHEMAS'),
        'object_type', d.defaclobjtype,
        'grantee', case when a.grantee = 0 then 'PUBLIC' else pg_get_userbyid(a.grantee) end,
        'privilege', a.privilege_type, 'grantable', a.is_grantable
      ) order by pg_get_userbyid(d.defaclrole), coalesce(n.nspname, ''), d.defaclobjtype, a.grantee, a.privilege_type), '[]'::jsonb)
       from pg_default_acl d left join pg_namespace n on n.oid = d.defaclnamespace
       cross join lateral aclexplode(d.defaclacl) a
       where n.nspname in ('public','private')
          or d.defaclrole in (select distinct relowner from objects where relowner is not null)) as relevant_default_acl,
      (select count(*)::integer from accessor_functions) as accessor_function_count,
      (select coalesce(jsonb_agg(jsonb_build_object(
        'schema', f.schema_name, 'function', f.function_name, 'identity_arguments', f.identity_arguments,
        'owner', f.owner, 'security_definer', f.security_definer, 'configuration', f.configuration,
        'effective_execute', (select jsonb_object_agg(r.role_name,
          case when r.role_oid is null then null else has_function_privilege(r.role_oid, f.oid, 'EXECUTE') end order by r.ordinal) from roles r)
      ) order by f.schema_name, f.function_name, f.identity_arguments), '[]'::jsonb) from accessor_functions f) as accessor_functions,
      (select coalesce(jsonb_agg(jsonb_build_object(
        'database', case when s.setdatabase = 0 then 'ALL_DATABASES' else (select datname from pg_database where oid = s.setdatabase) end,
        'role', case when s.setrole = 0 then 'ALL_ROLES' else pg_get_userbyid(s.setrole) end,
        'setting', setting
      ) order by s.setdatabase, s.setrole, setting), '[]'::jsonb)
       from pg_db_role_setting s cross join lateral unnest(s.setconfig) setting
       where split_part(setting, '=', 1) in ('pgrst.db_schemas','pgrst.db_anon_role','pgrst.db_extra_search_path','role','row_security')) as safe_role_and_api_settings
  `);
  return response.rows[0] as Record<string, unknown>;
}

async function probeBankingPayR11ApiBoundary(env: Env) {
  const probes: Array<Record<string, unknown>> = [];
  const base = new URL(env.MIGET_POSTGREST_ORIGIN);
  for (const [schemaName, objectName] of BANKING_PAY_R11_OBJECTS) {
    const target = new URL(base);
    target.pathname = `${target.pathname.replace(/\/$/, "")}/${encodeURIComponent(objectName)}`;
    target.searchParams.set("select", "*");
    target.searchParams.set("limit", "0");
    const headers = new Headers({ accept: "application/json" });
    if (schemaName !== "public") headers.set("accept-profile", schemaName);
    let status: number | null = null;
    let postgrestCode: string | null = null;
    try {
      const response = await fetch(target, {
        method: "GET",
        headers,
        redirect: "manual",
        signal: AbortSignal.timeout(8_000),
      });
      status = response.status;
      const bytes = new Uint8Array(await response.arrayBuffer());
      if (bytes.byteLength <= 65_536 && bytes.byteLength > 0) {
        try {
          const parsed = JSON.parse(new TextDecoder().decode(bytes));
          if (isRecord(parsed) && typeof parsed.code === "string") postgrestCode = parsed.code;
        } catch {
          postgrestCode = null;
        }
      }
    } catch {
      status = null;
      postgrestCode = "PROBE_TRANSPORT_FAILED";
    }
    probes.push({
      schema: schemaName,
      object: objectName,
      method: "GET",
      requested_row_limit: 0,
      authorization_supplied: false,
      http_status: status,
      postgrest_code: postgrestCode,
      response_body_returned: false,
    });
  }
  return probes;
}

function relationMetadataSql(section: z.infer<typeof RELATION_METADATA_SECTION>): string {
  const base = `
    with
    requested_targets as (
      select schema_name, relation_name, ordinal::integer
      from unnest($1::text[], $2::text[]) with ordinality input(schema_name, relation_name, ordinal)
    ),
    requested_roles as (
      select role_name, ordinal::integer
      from unnest($3::text[]) with ordinality input(role_name, ordinal)
    ),
    targets as (
      select i.schema_name, i.relation_name, i.ordinal,
        n.oid as schema_oid, c.oid as relation_oid, c.relowner, c.relkind,
        c.relpersistence, c.relrowsecurity, c.relforcerowsecurity,
        c.relreplident, c.relacl
      from requested_targets i
      left join pg_namespace n on n.nspname = i.schema_name
      left join pg_class c on c.relnamespace = n.oid
        and c.relname = i.relation_name
        and c.relkind in ('r','p','v','m','f','S')
    ),
    roles as (
      select i.role_name, i.ordinal, r.oid as role_oid, r.rolcanlogin,
        r.rolsuper, r.rolinherit, r.rolbypassrls
      from requested_roles i
      left join pg_roles r on r.rolname = i.role_name
    )`;

  let records: string;
  switch (section) {
    case "relations":
      records = `
        select format('%08s', t.ordinal) as sort_key,
          jsonb_build_object(
            'schema', t.schema_name,
            'relation', t.relation_name,
            'present', t.relation_oid is not null,
            'relation_kind', case t.relkind
              when 'r' then 'table' when 'p' then 'partitioned_table'
              when 'v' then 'view' when 'm' then 'materialized_view'
              when 'f' then 'foreign_table' when 'S' then 'sequence' else null end,
            'persistence', case t.relpersistence
              when 'p' then 'permanent' when 'u' then 'unlogged' when 't' then 'temporary' else null end,
            'owner', case when t.relowner is null then null else pg_get_userbyid(t.relowner) end,
            'rls_enabled', t.relrowsecurity,
            'rls_forced', t.relforcerowsecurity,
            'replica_identity', case t.relreplident
              when 'd' then 'default' when 'n' then 'nothing' when 'f' then 'full' when 'i' then 'index' else null end,
            'column_count', case when t.relation_oid is null then 0 else (
              select count(*)::integer from pg_attribute a
              where a.attrelid = t.relation_oid and a.attnum > 0 and not a.attisdropped) end,
            'trigger_count', case when t.relation_oid is null then 0 else (
              select count(*)::integer from pg_trigger g where g.tgrelid = t.relation_oid) end,
            'policy_count', case when t.relation_oid is null then 0 else (
              select count(*)::integer from pg_policy p where p.polrelid = t.relation_oid) end
          ) as record
        from targets t`;
      break;
    case "columns":
      records = `
        select format('%08s:%08s', t.ordinal, a.attnum) as sort_key,
          jsonb_build_object(
            'schema', t.schema_name, 'relation', t.relation_name,
            'ordinal', a.attnum, 'column', a.attname,
            'data_type', format_type(a.atttypid, a.atttypmod),
            'not_null', a.attnotnull, 'has_default', d.adbin is not null,
            'default_expression', pg_get_expr(d.adbin, d.adrelid),
            'identity', nullif(a.attidentity, ''),
            'generated', nullif(a.attgenerated, ''),
            'collation', case when a.attcollation = 0 then null else col.collname end,
            'compression', nullif(a.attcompression, ''),
            'statistics_target', a.attstattarget
          ) as record
        from targets t
        join pg_attribute a on a.attrelid = t.relation_oid and a.attnum > 0 and not a.attisdropped
        left join pg_attrdef d on d.adrelid = a.attrelid and d.adnum = a.attnum
        left join pg_collation col on col.oid = a.attcollation`;
      break;
    case "triggers":
      records = `
        select format('%08s:%s', t.ordinal, g.tgname) as sort_key,
          jsonb_build_object(
            'schema', t.schema_name, 'relation', t.relation_name,
            'trigger', g.tgname, 'internal', g.tgisinternal,
            'enabled', case g.tgenabled when 'O' then 'origin_and_local'
              when 'D' then 'disabled' when 'R' then 'replica' when 'A' then 'always' else g.tgenabled::text end,
            'constraint_trigger', g.tgconstraint <> 0,
            'constraint', con.conname,
            'definition', pg_get_triggerdef(g.oid, true),
            'function_schema', fn.nspname,
            'function_name', p.proname,
            'function_identity_arguments', pg_get_function_identity_arguments(p.oid),
            'function_owner', pg_get_userbyid(p.proowner),
            'function_security_definer', p.prosecdef,
            'function_configuration', coalesce((
              select jsonb_agg(setting order by setting)
              from unnest(p.proconfig) setting
              where split_part(setting, '=', 1) in (
                'search_path', 'row_security', 'statement_timeout',
                'lock_timeout', 'idle_in_transaction_session_timeout'
              )
            ), '[]'::jsonb)
          ) as record
        from targets t
        join pg_trigger g on g.tgrelid = t.relation_oid
        join pg_proc p on p.oid = g.tgfoid
        join pg_namespace fn on fn.oid = p.pronamespace
        left join pg_constraint con on con.oid = g.tgconstraint`;
      break;
    case "rules":
      records = `
        select format('%08s:%s', t.ordinal, r.rulename) as sort_key,
          jsonb_build_object(
            'schema', t.schema_name, 'relation', t.relation_name,
            'rule', r.rulename,
            'enabled', case r.ev_enabled when 'O' then 'origin_and_local'
              when 'D' then 'disabled' when 'R' then 'replica' when 'A' then 'always' else r.ev_enabled::text end,
            'event', case r.ev_type when '1' then 'select' when '2' then 'update'
              when '3' then 'insert' when '4' then 'delete' else r.ev_type::text end,
            'instead', r.is_instead,
            'definition', pg_get_ruledef(r.oid, true)
          ) as record
        from targets t
        join pg_rewrite r on r.ev_class = t.relation_oid`;
      break;
    case "constraints":
      records = `
        select format('%s:%s:%s', source_ns.nspname, source.relname, c.conname) as sort_key,
          jsonb_build_object(
            'constraint', c.conname,
            'type', case c.contype when 'c' then 'check' when 'f' then 'foreign_key'
              when 'p' then 'primary_key' when 'u' then 'unique' when 't' then 'constraint_trigger'
              when 'x' then 'exclusion' else c.contype::text end,
            'source_schema', source_ns.nspname, 'source_relation', source.relname,
            'target_schema', target_ns.nspname, 'target_relation', target.relname,
            'deferrable', c.condeferrable, 'initially_deferred', c.condeferred,
            'validated', c.convalidated,
            'definition', pg_get_constraintdef(c.oid, true)
          ) as record
        from pg_constraint c
        join pg_class source on source.oid = c.conrelid
        join pg_namespace source_ns on source_ns.oid = source.relnamespace
        left join pg_class target on target.oid = c.confrelid
        left join pg_namespace target_ns on target_ns.oid = target.relnamespace
        where exists (select 1 from targets t where t.relation_oid in (c.conrelid, c.confrelid))`;
      break;
    case "foreign_keys":
      records = `
        select format('%s:%s:%s', source_ns.nspname, source.relname, c.conname) as sort_key,
          jsonb_build_object(
            'constraint', c.conname,
            'source_schema', source_ns.nspname, 'source_relation', source.relname,
            'target_schema', target_ns.nspname, 'target_relation', target.relname,
            'source_columns', (select jsonb_agg(a.attname order by key.ordinality)
              from unnest(c.conkey) with ordinality key(attnum, ordinality)
              join pg_attribute a on a.attrelid = c.conrelid and a.attnum = key.attnum),
            'target_columns', (select jsonb_agg(a.attname order by key.ordinality)
              from unnest(c.confkey) with ordinality key(attnum, ordinality)
              join pg_attribute a on a.attrelid = c.confrelid and a.attnum = key.attnum),
            'match_type', case c.confmatchtype when 'f' then 'full' when 'p' then 'partial' else 'simple' end,
            'on_update', case c.confupdtype when 'a' then 'no_action' when 'r' then 'restrict'
              when 'c' then 'cascade' when 'n' then 'set_null' when 'd' then 'set_default' else c.confupdtype::text end,
            'on_delete', case c.confdeltype when 'a' then 'no_action' when 'r' then 'restrict'
              when 'c' then 'cascade' when 'n' then 'set_null' when 'd' then 'set_default' else c.confdeltype::text end,
            'deferrable', c.condeferrable, 'initially_deferred', c.condeferred,
            'validated', c.convalidated,
            'definition', pg_get_constraintdef(c.oid, true)
          ) as record
        from pg_constraint c
        join pg_class source on source.oid = c.conrelid
        join pg_namespace source_ns on source_ns.oid = source.relnamespace
        join pg_class target on target.oid = c.confrelid
        join pg_namespace target_ns on target_ns.oid = target.relnamespace
        where c.contype = 'f'
          and exists (select 1 from targets t where t.relation_oid in (c.conrelid, c.confrelid))`;
      break;
    case "dependencies":
      records = `
        with related_objects as (
          select 'pg_class'::regclass::oid as class_id, t.relation_oid as object_id, 0::integer as sub_id
          from targets t where t.relation_oid is not null
          union
          select 'pg_trigger'::regclass::oid, g.oid, 0 from targets t join pg_trigger g on g.tgrelid = t.relation_oid
          union
          select 'pg_rewrite'::regclass::oid, r.oid, 0 from targets t join pg_rewrite r on r.ev_class = t.relation_oid
          union
          select 'pg_constraint'::regclass::oid, c.oid, 0 from pg_constraint c
          where exists (select 1 from targets t where t.relation_oid in (c.conrelid, c.confrelid))
          union
          select 'pg_class'::regclass::oid, i.indexrelid, 0 from targets t join pg_index i on i.indrelid = t.relation_oid
        ), relevant as (
          select distinct d.classid, d.objid, d.objsubid, d.refclassid, d.refobjid, d.refobjsubid, d.deptype
          from pg_depend d
          where exists (select 1 from related_objects o where o.class_id = d.classid and o.object_id = d.objid)
             or exists (select 1 from related_objects o where o.class_id = d.refclassid and o.object_id = d.refobjid)
        )
        select format('%010s:%010s:%010s:%010s', d.classid, d.objid, d.refclassid, d.refobjid) as sort_key,
          jsonb_build_object(
            'dependency_type', case d.deptype when 'n' then 'normal' when 'a' then 'auto'
              when 'i' then 'internal' when 'P' then 'partition_primary'
              when 'S' then 'partition_secondary' when 'e' then 'extension' else d.deptype::text end,
            'dependent', jsonb_build_object('type', dependent.type, 'schema', dependent.schema,
              'name', dependent.name, 'identity', dependent.identity, 'subobject_id', d.objsubid),
            'referenced', jsonb_build_object('type', referenced.type, 'schema', referenced.schema,
              'name', referenced.name, 'identity', referenced.identity, 'subobject_id', d.refobjsubid)
          ) as record
        from relevant d
        cross join lateral pg_identify_object(d.classid, d.objid, d.objsubid) dependent
        cross join lateral pg_identify_object(d.refclassid, d.refobjid, d.refobjsubid) referenced`;
      break;
    case "privileges":
      records = `
        select format('direct:%08s:%010s:%s', t.ordinal, acl.grantee, acl.privilege_type) as sort_key,
          jsonb_build_object(
            'record_kind', 'direct_acl', 'schema', t.schema_name, 'relation', t.relation_name,
            'acl_source', case when t.relacl is null then 'postgres_default' else 'relation_acl' end,
            'grantor', pg_get_userbyid(acl.grantor),
            'grantee', case when acl.grantee = 0 then 'PUBLIC' else pg_get_userbyid(acl.grantee) end,
            'privilege', acl.privilege_type, 'grantable', acl.is_grantable
          ) as record
        from targets t
        cross join lateral aclexplode(coalesce(t.relacl, acldefault('r', t.relowner))) acl
        where t.relation_oid is not null
        union all
        select format('effective:%08s:%08s', t.ordinal, r.ordinal) as sort_key,
          jsonb_build_object(
            'record_kind', 'effective_role_access', 'schema', t.schema_name, 'relation', t.relation_name,
            'relation_present', t.relation_oid is not null, 'role', r.role_name,
            'role_present', r.role_oid is not null, 'can_login', r.rolcanlogin,
            'superuser', r.rolsuper, 'inherits', r.rolinherit, 'bypass_rls', r.rolbypassrls,
            'owner_match', case when r.role_oid is null or t.relation_oid is null then null else r.role_oid = t.relowner end,
            'schema_usage', case when r.role_oid is null or t.schema_oid is null then null else has_schema_privilege(r.role_oid, t.schema_oid, 'USAGE') end,
            'select', case when r.role_oid is null or t.relation_oid is null then null else has_table_privilege(r.role_oid, t.relation_oid, 'SELECT') end,
            'insert', case when r.role_oid is null or t.relation_oid is null then null else has_table_privilege(r.role_oid, t.relation_oid, 'INSERT') end,
            'update', case when r.role_oid is null or t.relation_oid is null then null else has_table_privilege(r.role_oid, t.relation_oid, 'UPDATE') end,
            'delete', case when r.role_oid is null or t.relation_oid is null then null else has_table_privilege(r.role_oid, t.relation_oid, 'DELETE') end,
            'truncate', case when r.role_oid is null or t.relation_oid is null then null else has_table_privilege(r.role_oid, t.relation_oid, 'TRUNCATE') end,
            'references', case when r.role_oid is null or t.relation_oid is null then null else has_table_privilege(r.role_oid, t.relation_oid, 'REFERENCES') end,
            'trigger', case when r.role_oid is null or t.relation_oid is null then null else has_table_privilege(r.role_oid, t.relation_oid, 'TRIGGER') end,
            'rls_enabled', t.relrowsecurity, 'rls_forced', t.relforcerowsecurity,
            'rls_bypass_by_attribute_or_owner', case when r.role_oid is null or t.relation_oid is null then null
              else r.rolsuper or r.rolbypassrls or (r.role_oid = t.relowner and not t.relforcerowsecurity) end
          ) as record
        from targets t cross join roles r`;
      break;
    case "policies":
      records = `
        select format('%08s:%s', t.ordinal, p.polname) as sort_key,
          jsonb_build_object(
            'schema', t.schema_name, 'relation', t.relation_name,
            'policy', p.polname, 'permissive', p.polpermissive,
            'command', case p.polcmd when 'r' then 'select' when 'a' then 'insert'
              when 'w' then 'update' when 'd' then 'delete' when '*' then 'all' else p.polcmd::text end,
            'roles', (select coalesce(jsonb_agg(case when role_id = 0 then 'PUBLIC' else pg_get_userbyid(role_id) end order by role_id), '[]'::jsonb)
              from unnest(p.polroles) role_id),
            'using_expression', pg_get_expr(p.polqual, p.polrelid),
            'check_expression', pg_get_expr(p.polwithcheck, p.polrelid)
          ) as record
        from targets t join pg_policy p on p.polrelid = t.relation_oid`;
      break;
    case "indexes":
      records = `
        select format('%08s:%s', t.ordinal, idx.relname) as sort_key,
          jsonb_build_object(
            'schema', t.schema_name, 'relation', t.relation_name,
            'index_schema', idx_ns.nspname, 'index', idx.relname,
            'index_binding_contract', 'CLOUDTMS_INDEX_OPERATOR_BINDINGS_V1',
            'owner', pg_get_userbyid(idx.relowner),
            'access_method', am.amname,
            'unique', i.indisunique, 'primary', i.indisprimary,
            'exclusion', i.indisexclusion, 'immediate', i.indimmediate,
            'valid', i.indisvalid, 'ready', i.indisready, 'live', i.indislive,
            'clustered', i.indisclustered, 'replica_identity', i.indisreplident,
            'key_attribute_count', i.indnkeyatts,
            'total_attribute_count', i.indnatts,
            'definition', pg_get_indexdef(i.indexrelid),
            'predicate', pg_get_expr(i.indpred, i.indrelid),
            'keys', (
              select coalesce(jsonb_agg(jsonb_build_object(
                'position', key.position,
                'attribute_number', key.attribute_number,
                'column', table_attribute.attname,
                'definition', pg_get_indexdef(i.indexrelid, key.position, true),
                'source_data_type', format_type(table_attribute.atttypid, table_attribute.atttypmod),
                'stored_data_type', format_type(index_attribute.atttypid, index_attribute.atttypmod),
                'collation_schema', collation_ns.nspname,
                'collation', collation_row.collname,
                'operator_class_schema', opclass_ns.nspname,
                'operator_class', opclass.opcname,
                'operator_class_is_default', opclass.opcdefault,
                'operator_class_input_type', format_type(opclass.opcintype, null),
                'operator_family_schema', opfamily_ns.nspname,
                'operator_family', opfamily.opfname,
                'support_functions', (
                  select coalesce(jsonb_agg(jsonb_build_object(
                    'support_number', support.amprocnum,
                    'left_type', format_type(support.amproclefttype, null),
                    'right_type', format_type(support.amprocrighttype, null),
                    'exact_operator_class_input_types',
                      support.amproclefttype = opclass.opcintype
                      and support.amprocrighttype = opclass.opcintype,
                    'function_schema', support_ns.nspname,
                    'function', support_proc.proname,
                    'function_identity_arguments', pg_get_function_identity_arguments(support_proc.oid),
                    'function_result', pg_get_function_result(support_proc.oid)
                  ) order by support.amprocnum, support.amproclefttype, support.amprocrighttype,
                    support_ns.nspname, support_proc.proname,
                    pg_get_function_identity_arguments(support_proc.oid)), '[]'::jsonb)
                  from pg_amproc support
                  join pg_proc support_proc on support_proc.oid = support.amproc
                  join pg_namespace support_ns on support_ns.oid = support_proc.pronamespace
                  where support.amprocfamily = opclass.opcfamily
                )
              ) order by key.position), '[]'::jsonb)
              from (
                select
                  position,
                  i.indkey[position - 1]::integer as attribute_number,
                  i.indclass[position - 1]::oid as operator_class_oid,
                  i.indcollation[position - 1]::oid as collation_oid
                from generate_series(1, i.indnkeyatts) position
              ) key
              join pg_opclass opclass on opclass.oid = key.operator_class_oid
              join pg_namespace opclass_ns on opclass_ns.oid = opclass.opcnamespace
              join pg_opfamily opfamily on opfamily.oid = opclass.opcfamily
              join pg_namespace opfamily_ns on opfamily_ns.oid = opfamily.opfnamespace
              join pg_attribute index_attribute
                on index_attribute.attrelid = i.indexrelid
               and index_attribute.attnum = key.position
               and not index_attribute.attisdropped
              left join pg_attribute table_attribute
                on table_attribute.attrelid = i.indrelid
               and table_attribute.attnum = key.attribute_number
               and not table_attribute.attisdropped
              left join pg_collation collation_row on collation_row.oid = key.collation_oid
              left join pg_namespace collation_ns on collation_ns.oid = collation_row.collnamespace
            )
          ) as record
        from targets t
        join pg_index i on i.indrelid = t.relation_oid
        join pg_class idx on idx.oid = i.indexrelid
        join pg_namespace idx_ns on idx_ns.oid = idx.relnamespace
        join pg_am am on am.oid = idx.relam
        where ($6::text[] is null or idx.relname = any($6::text[]))`;
      break;
  }

  return `${base},
    records as (${records}),
    page as (
      select sort_key, record from records order by sort_key limit $4 offset $5
    )
    select
      (select count(*)::integer from records) as total,
      coalesce((select jsonb_agg(record order by sort_key) from page), '[]'::jsonb) as items`;
}

async function collectRelationMetadata(
  client: Client,
  relations: Array<{ schema: string; relation: string }>,
  roles: string[],
  section: z.infer<typeof RELATION_METADATA_SECTION>,
  limit: number,
  offset: number,
  indexNames?: string[],
) {
  const values: unknown[] = [
    relations.map((target) => target.schema),
    relations.map((target) => target.relation),
    roles,
    limit,
    offset,
  ];
  if (section === "indexes") values.push(indexNames ?? null);
  const response = await client.query(relationMetadataSql(section), values);
  const total = Number(response.rows[0]?.total ?? 0);
  const items = Array.isArray(response.rows[0]?.items) ? response.rows[0].items : [];
  return {
    metadata_contract: "CLOUDTMS_RELATION_METADATA_V1",
    section,
    requested_relation_count: relations.length,
    requested_role_count: roles.length,
    requested_index_count: indexNames?.length ?? 0,
    index_filter_applied: section === "indexes" && indexNames !== undefined,
    total,
    limit,
    offset,
    returned: items.length,
    eof: offset + items.length >= total,
    next_offset: offset + items.length >= total ? null : offset + items.length,
    truncated: offset + items.length < total,
    business_rows_returned: 0,
    caller_supplied_sql_accepted: false,
    items,
  };
}

async function collectRoutineMetadata(
  client: Client,
  routines: Array<{ schema: string; name: string; identity_arguments: string }>,
  roles: string[],
  limit: number,
  offset: number,
) {
  const pageResponse = await client.query(
    `with requested as (
       select
         (entry.ordinality - 1)::integer as requested_ordinal,
         entry.item ->> 'schema' as schema_name,
         entry.item ->> 'name' as routine_name,
         entry.item ->> 'identity_arguments' as identity_arguments
       from jsonb_array_elements($1::jsonb) with ordinality as entry(item, ordinality)
     ), page as (
       select * from requested order by requested_ordinal limit $2 offset $3
     )
     select
       page.requested_ordinal,
       page.schema_name,
       page.routine_name,
       page.identity_arguments,
       p.oid::text as routine_oid,
       case p.prokind when 'p' then 'procedure' when 'w' then 'window_function' else 'function' end as kind,
       pg_get_userbyid(p.proowner) as owner,
       l.lanname as language,
       p.prosecdef as security_definer,
       case p.provolatile when 'i' then 'immutable' when 's' then 'stable' else 'volatile' end as volatility,
       p.proacl is null as acl_uses_postgresql_default,
       pg_get_function_result(p.oid) as result_type
     from page
     left join pg_namespace n on n.nspname = page.schema_name
     left join pg_proc p
       on p.pronamespace = n.oid
      and p.proname = page.routine_name
      and pg_get_function_identity_arguments(p.oid) = page.identity_arguments
      and p.prokind in ('f', 'p', 'w')
     left join pg_language l on l.oid = p.prolang
     order by page.requested_ordinal`,
    [JSON.stringify(routines), limit, offset],
  );

  const routineOids = pageResponse.rows
    .map((row) => row.routine_oid as string | null)
    .filter((oid): oid is string => Boolean(oid));

  let aclRows: Array<Record<string, unknown>> = [];
  let effectiveRows: Array<Record<string, unknown>> = [];
  if (routineOids.length > 0) {
    const aclResponse = await client.query(
      `select
         p.oid::text as routine_oid,
         case when acl.grantee = 0 then 'PUBLIC' else pg_get_userbyid(acl.grantee) end as grantee,
         pg_get_userbyid(acl.grantor) as grantor,
         acl.privilege_type,
         acl.is_grantable,
         p.proacl is null as derived_from_postgresql_default_acl
       from pg_proc p
       cross join lateral aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) acl
       where p.oid = any($1::oid[])
         and acl.privilege_type = 'EXECUTE'
       order by p.oid, grantee, grantor`,
      [routineOids],
    );
    aclRows = aclResponse.rows;

    const effectiveResponse = await client.query(
      `with requested_roles as (
         select role_name, ordinality
         from unnest($2::text[]) with ordinality as requested(role_name, ordinality)
       )
       select
         p.oid::text as routine_oid,
         requested_roles.role_name as role,
         r.oid is not null as role_present,
         case when r.oid is null then null else has_function_privilege(r.oid, p.oid, 'EXECUTE') end as effective_execute,
         case when r.oid is null then null else has_schema_privilege(r.oid, p.pronamespace, 'USAGE') end as effective_schema_usage,
         case when r.oid is null then null else
           has_function_privilege(r.oid, p.oid, 'EXECUTE')
           and has_schema_privilege(r.oid, p.pronamespace, 'USAGE')
         end as effective_callable
       from pg_proc p
       cross join requested_roles
       left join pg_roles r on r.rolname = requested_roles.role_name
       where p.oid = any($1::oid[])
       order by p.oid, requested_roles.ordinality`,
      [routineOids, roles],
    );
    effectiveRows = effectiveResponse.rows;
  }

  const roleResponse = await client.query(
    `with requested_roles as (
       select role_name, ordinality
       from unnest($1::text[]) with ordinality as requested(role_name, ordinality)
     )
     select
       requested_roles.role_name as role,
       r.oid is not null as role_present,
       r.rolcanlogin as can_login,
       r.rolsuper as superuser,
       r.rolinherit as inherits_by_default,
       r.rolbypassrls as bypass_rls
     from requested_roles
     left join pg_roles r on r.rolname = requested_roles.role_name
     order by requested_roles.ordinality`,
    [roles],
  );
  const membershipResponse = await client.query(
    `with recursive requested_roles as (
       select role_name, ordinality
       from unnest($1::text[]) with ordinality as requested(role_name, ordinality)
     ), roots as (
       select requested_roles.role_name, requested_roles.ordinality, r.oid
       from requested_roles join pg_roles r on r.rolname = requested_roles.role_name
     ), inherited(start_oid, inherited_oid, depth, path, admin_option, inherit_option, set_option) as (
       select roots.oid, roots.oid, 0, array[roots.oid], null::boolean, null::boolean, null::boolean
       from roots
       union all
       select
         inherited.start_oid,
         membership.roleid,
         inherited.depth + 1,
         inherited.path || membership.roleid,
         membership.admin_option,
         membership.inherit_option,
         membership.set_option
       from inherited
       join pg_auth_members membership on membership.member = inherited.inherited_oid
       where inherited.depth < 32
         and membership.inherit_option
         and not membership.roleid = any(inherited.path)
     )
     select
       pg_get_userbyid(inherited.start_oid) as requested_role,
       pg_get_userbyid(inherited.inherited_oid) as inherited_role,
       inherited.depth,
       (select array_agg(pg_get_userbyid(path_item.oid) order by path_item.ordinality)
        from unnest(inherited.path) with ordinality as path_item(oid, ordinality)) as path,
       inherited.admin_option,
       inherited.inherit_option,
       inherited.set_option
     from inherited
     order by requested_role, inherited.depth, inherited_role`,
    [roles],
  );

  const aclByRoutine = new Map<string, Array<Record<string, unknown>>>();
  for (const row of aclRows) {
    const oid = String(row.routine_oid);
    const list = aclByRoutine.get(oid) ?? [];
    list.push(row);
    aclByRoutine.set(oid, list);
  }
  const effectiveByRoutine = new Map<string, Array<Record<string, unknown>>>();
  for (const row of effectiveRows) {
    const oid = String(row.routine_oid);
    const list = effectiveByRoutine.get(oid) ?? [];
    list.push(row);
    effectiveByRoutine.set(oid, list);
  }
  const items = pageResponse.rows.map((row) => {
    const oid = row.routine_oid as string | null;
    return {
      requested_ordinal: row.requested_ordinal,
      schema: row.schema_name,
      name: row.routine_name,
      identity_arguments: row.identity_arguments,
      present: Boolean(oid),
      routine_oid: oid,
      kind: row.kind,
      owner: row.owner,
      language: row.language,
      security_definer: row.security_definer,
      volatility: row.volatility,
      result_type: row.result_type,
      acl_uses_postgresql_default: row.acl_uses_postgresql_default,
      direct_or_default_execute_acl: oid ? (aclByRoutine.get(oid) ?? []) : [],
      effective_roles: oid ? (effectiveByRoutine.get(oid) ?? []) : [],
    };
  });
  const nextOffset = offset + items.length;
  return {
    metadata_contract: "CLOUDTMS_ROUTINE_METADATA_V1",
    requested_routine_count: routines.length,
    requested_role_count: roles.length,
    total: routines.length,
    limit,
    offset,
    returned: items.length,
    eof: nextOffset >= routines.length,
    next_offset: nextOffset >= routines.length ? null : nextOffset,
    truncated: nextOffset < routines.length,
    business_rows_returned: 0,
    caller_supplied_sql_accepted: false,
    effective_execute_source: "pg_catalog.has_function_privilege(role_oid, routine_oid, EXECUTE)",
    effective_schema_usage_source: "pg_catalog.has_schema_privilege(role_oid, schema_oid, USAGE)",
    roles: roleResponse.rows,
    effective_membership_paths: membershipResponse.rows,
    items,
  };
}

function databaseCodeSearchSql(): string {
  return `
    with definitions as (
      select
        case p.prokind when 'p' then 'procedure' when 'w' then 'window_function' else 'function' end::text as object_kind,
        n.nspname::text as schema_name,
        p.proname::text as object_name,
        pg_get_function_identity_arguments(p.oid)::text as identity,
        null::text as parent_schema,
        null::text as parent_name,
        l.lanname::text as language,
        p.prosecdef::boolean as security_definer,
        pg_get_functiondef(p.oid)::text as definition
      from pg_proc p
      join pg_namespace n on n.oid = p.pronamespace
      join pg_language l on l.oid = p.prolang
      where p.prokind in ('f', 'p', 'w')
        and n.nspname not in ('pg_catalog', 'information_schema')
        and n.nspname !~ '^pg_toast'

      union all
      select
        case c.relkind when 'm' then 'materialized_view' else 'view' end,
        n.nspname, c.relname, ''::text,
        null::text, null::text, 'sql'::text, false,
        pg_get_viewdef(c.oid, true)::text
      from pg_class c
      join pg_namespace n on n.oid = c.relnamespace
      where c.relkind in ('v', 'm')
        and n.nspname not in ('pg_catalog', 'information_schema')
        and n.nspname !~ '^pg_toast'

      union all
      select
        'rule', n.nspname, r.rulename,
        format('%I.%I', n.nspname, c.relname),
        n.nspname, c.relname, 'sql', false,
        pg_get_ruledef(r.oid, true)::text
      from pg_rewrite r
      join pg_class c on c.oid = r.ev_class
      join pg_namespace n on n.oid = c.relnamespace
      where n.nspname not in ('pg_catalog', 'information_schema')
        and n.nspname !~ '^pg_toast'

      union all
      select
        'trigger', n.nspname, t.tgname,
        format('%I.%I', fn.nspname, p.proname),
        n.nspname, c.relname, l.lanname, p.prosecdef,
        pg_get_triggerdef(t.oid, true)::text
      from pg_trigger t
      join pg_class c on c.oid = t.tgrelid
      join pg_namespace n on n.oid = c.relnamespace
      join pg_proc p on p.oid = t.tgfoid
      join pg_namespace fn on fn.oid = p.pronamespace
      join pg_language l on l.oid = p.prolang
      where n.nspname not in ('pg_catalog', 'information_schema')
        and n.nspname !~ '^pg_toast'

      union all
      select
        'policy', n.nspname, p.polname,
        format('%I.%I', n.nspname, c.relname),
        n.nspname, c.relname, 'sql', false,
        concat_ws(E'\\n',
          format('POLICY %I ON %I.%I', p.polname, n.nspname, c.relname),
          format('COMMAND %s', p.polcmd),
          format('ROLES %s', array_to_string(p.polroles, ',')),
          format('USING %s', coalesce(pg_get_expr(p.polqual, p.polrelid, true), '<NULL>')),
          format('WITH CHECK %s', coalesce(pg_get_expr(p.polwithcheck, p.polrelid, true), '<NULL>'))
        )::text
      from pg_policy p
      join pg_class c on c.oid = p.polrelid
      join pg_namespace n on n.oid = c.relnamespace
      where n.nspname not in ('pg_catalog', 'information_schema')
        and n.nspname !~ '^pg_toast'

      union all
      select
        'index', n.nspname, idx.relname,
        format('%I.%I', n.nspname, parent.relname),
        n.nspname, parent.relname, 'sql', false,
        pg_get_indexdef(i.indexrelid)::text
      from pg_index i
      join pg_class idx on idx.oid = i.indexrelid
      join pg_class parent on parent.oid = i.indrelid
      join pg_namespace n on n.oid = parent.relnamespace
      where n.nspname not in ('pg_catalog', 'information_schema')
        and n.nspname !~ '^pg_toast'

      union all
      select
        'constraint', n.nspname, con.conname,
        format('%I.%I', n.nspname, c.relname),
        n.nspname, c.relname, 'sql', false,
        pg_get_constraintdef(con.oid, true)::text
      from pg_constraint con
      join pg_class c on c.oid = con.conrelid
      join pg_namespace n on n.oid = c.relnamespace
      where n.nspname not in ('pg_catalog', 'information_schema')
        and n.nspname !~ '^pg_toast'

      union all
      select
        'column_expression', n.nspname, a.attname,
        format('%I.%I.%I', n.nspname, c.relname, a.attname),
        n.nspname, c.relname, 'sql', false,
        pg_get_expr(d.adbin, d.adrelid, true)::text
      from pg_attrdef d
      join pg_class c on c.oid = d.adrelid
      join pg_namespace n on n.oid = c.relnamespace
      join pg_attribute a on a.attrelid = d.adrelid and a.attnum = d.adnum
      where n.nspname not in ('pg_catalog', 'information_schema')
        and n.nspname !~ '^pg_toast'
        and not a.attisdropped

      union all
      select
        'event_trigger', fn.nspname, e.evtname,
        format('%I.%I(%s)', fn.nspname, p.proname, pg_get_function_identity_arguments(p.oid)),
        null::text, null::text, l.lanname, p.prosecdef,
        concat_ws(E'\\n',
          format('EVENT %s', e.evtevent),
          format('TAGS %s', coalesce(array_to_string(e.evttags, ','), '<ALL>')),
          format('FUNCTION %I.%I(%s)', fn.nspname, p.proname, pg_get_function_identity_arguments(p.oid))
        )::text
      from pg_event_trigger e
      join pg_proc p on p.oid = e.evtfoid
      join pg_namespace fn on fn.oid = p.pronamespace
      join pg_language l on l.oid = p.prolang
      where fn.nspname not in ('pg_catalog', 'information_schema')
        and fn.nspname !~ '^pg_toast'
    ), filtered as (
      select d.*
      from definitions d
      where ($2::text[] is null or d.schema_name = any($2::text[]))
        and ($3::text[] is null or d.object_kind = any($3::text[]))
        and (
          ($4::text = 'all' and not exists (
            select 1 from unnest($1::text[]) term
            where strpos(lower(d.definition), lower(term)) = 0
          ))
          or ($4::text = 'any' and exists (
            select 1 from unnest($1::text[]) term
            where strpos(lower(d.definition), lower(term)) > 0
          ))
        )
    ), kind_totals as (
      select object_kind, count(*)::integer as total
      from filtered
      group by object_kind
    ), page as (
      select * from filtered
      order by object_kind, schema_name, object_name, identity
      limit $5 offset $6
    )
    select
      (select count(*)::integer from filtered) as total,
      coalesce((select jsonb_object_agg(object_kind, total order by object_kind) from kind_totals), '{}'::jsonb) as kind_totals,
      coalesce((
        select jsonb_agg(jsonb_build_object(
          'object_kind', d.object_kind,
          'schema', d.schema_name,
          'name', d.object_name,
          'identity', d.identity,
          'parent_schema', d.parent_schema,
          'parent_name', d.parent_name,
          'language', d.language,
          'security_definer', d.security_definer,
          'definition_characters', char_length(d.definition),
          'definition_bytes', octet_length(d.definition),
          'definition_sha256', encode(sha256(convert_to(d.definition, 'UTF8')), 'hex'),
          'matches', (
            select coalesce(jsonb_agg(jsonb_build_object(
              'term', match.term,
              'character_position', match.position,
              'excerpt', substring(d.definition from greatest(match.position - 120, 1) for $7)
            ) order by match.ordinality), '[]'::jsonb)
            from (
              select term, ordinality, strpos(lower(d.definition), lower(term)) as position
              from unnest($1::text[]) with ordinality searched(term, ordinality)
            ) match
            where match.position > 0
          )
        ) order by d.object_kind, d.schema_name, d.object_name, d.identity)
        from page d
      ), '[]'::jsonb) as items
  `;
}

async function searchDatabaseCode(
  client: Client,
  terms: string[],
  schemas: string[] | undefined,
  kinds: Array<z.infer<typeof DATABASE_CODE_KIND>> | undefined,
  matchMode: "any" | "all",
  limit: number,
  offset: number,
  excerptCharacters: number,
) {
  const response = await client.query(databaseCodeSearchSql(), [
    terms,
    schemas ?? null,
    kinds ?? null,
    matchMode,
    limit,
    offset,
    excerptCharacters,
  ]);
  const total = Number(response.rows[0]?.total ?? 0);
  const items = Array.isArray(response.rows[0]?.items) ? response.rows[0].items : [];
  const nextOffset = offset + items.length;
  return {
    search_contract: "CLOUDTMS_TEST_DATABASE_CODE_SEARCH_V1",
    matching: "case-insensitive literal substring",
    match_mode: matchMode,
    terms,
    schemas: schemas ?? null,
    object_kinds: kinds ?? [...DATABASE_CODE_KIND.options],
    total,
    kind_totals: response.rows[0]?.kind_totals ?? {},
    limit,
    offset,
    returned: items.length,
    eof: nextOffset >= total,
    next_offset: nextOffset >= total ? null : nextOffset,
    truncated: nextOffset < total,
    excerpt_characters: excerptCharacters,
    complete_definition_returned: false,
    definition_sha256_covers_complete_definition: true,
    business_rows_returned: 0,
    caller_supplied_sql_accepted: false,
    items,
  };
}

function createServer(env: Env, transactionRollbackAvailable = false): McpServer {
  const allowedDatabaseTarget = env.MIGET_TEST_ONLY === "true"
    ? TEST_DATABASE_TARGET
    : DATABASE_TARGET;
  const databaseScopeDescription = env.MIGET_TEST_ONLY === "true"
    ? "agency_test or mytms_test"
    : "agency_test, mytms_test, or agency_live";
  const server = new McpServer({
    name: transactionRollbackAvailable
      ? "CloudTMS Miget Operations with TEST rollback rehearsal"
      : "CloudTMS Miget Read-Only Operations",
    version: "1.0.0",
  });

  if (transactionRollbackAvailable) {
    server.registerTool(
      "miget_db_transaction_rollback",
      {
        title: "Rehearse SQL/RPC changes on TEST and always roll back",
        description:
          "Runs a bounded SQL batch on either agency TEST or MyTMS TEST inside one server-owned PostgreSQL transaction and unconditionally rolls it back. Optional verification SQL then runs on a fresh read-only connection. LIVE is not a valid target. Outer transaction escape commands are refused. Do not invoke payment, provider, email, network, or other external-side-effect functions.",
        inputSchema: z.object({
          database: TEST_DATABASE_TARGET,
          label: z.string().trim().min(1).max(120),
          sql: z.string().min(1).max(8_000_000),
          verification_sql: z.string().min(1).max(1_000_000).optional(),
          statement_timeout_ms: z.number().int().min(100).max(120_000).default(30_000),
          lock_timeout_ms: z.number().int().min(100).max(10_000).default(2_000),
          max_rows_per_statement: z.number().int().min(0).max(100).default(25),
          continue_on_error: z.boolean().default(false),
        }).strict(),
        annotations: ROLLBACK_ONLY_ANNOTATIONS,
      },
      async ({
        database,
        label,
        sql,
        verification_sql,
        statement_timeout_ms,
        lock_timeout_ms,
        max_rows_per_statement,
        continue_on_error,
      }) => {
        const startedAt = Date.now();
        const hyperdrive = database === "mytms_test" ? env.MYTMS_HYPERDRIVE : env.HYPERDRIVE;
        const expectedDatabase = database === "mytms_test" ? "uofvkfi5" : "cloudtms_test_clone";
        const result = await runRollbackTransaction(
          () => new Client({
            connectionString: hyperdrive.connectionString,
            application_name: `cloudtms-kier-codex-rollback-${database}`,
            connectionTimeoutMillis: 10_000,
            query_timeout: statement_timeout_ms + 5_000,
          }),
          {
            databaseTarget: database,
            expectedDatabase,
            label,
            sql,
            verificationSql: verification_sql,
            statementTimeoutMs: statement_timeout_ms,
            lockTimeoutMs: lock_timeout_ms,
            maxRowsPerStatement: max_rows_per_statement,
            continueOnError: continue_on_error,
          },
        );
        console.log(JSON.stringify({
          message: "Miget TEST rollback rehearsal completed",
          database,
          label,
          sql_sha256: result.sqlSha256,
          statement_count: result.statementCount,
          accepted: result.accepted,
          rolled_back: result.rolledBack,
          elapsed_ms: Date.now() - startedAt,
        }));
        return resultContent(result as unknown as Record<string, unknown>);
      },
    );
  }

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
      if (env.MIGET_TEST_ONLY === "true") {
        const [resource, agencyService, mytmsService] = await Promise.all([
          migetGet(env, `/resources/${env.MIGET_RESOURCE_ID}`),
          migetGet(env, `/services/${env.MIGET_POSTGRES_SERVICE_ID}`),
          migetGet(env, `/services/${env.MYTMS_MIGET_POSTGRES_SERVICE_ID}`),
        ]);
        return resultContent({
          authenticated: true,
          read_only: true,
          scope: "cloudtms-miget-test-only",
          projects: [],
          resources: safeCollection(resource),
          services: [...safeCollection(agencyService), ...safeCollection(mytmsService)],
        });
      }
      const [projects, resources, services] = await Promise.all([
        migetGet(env, "/projects"),
        migetGet(env, "/resources"),
        migetGet(env, "/services"),
      ]);
      return resultContent({
        authenticated: true,
        read_only: true,
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
        `Read-only. Inspects ${databaseScopeDescription}. Passwords, connection strings, and other secret fields are never returned.`,
      inputSchema: z.object({
        database: allowedDatabaseTarget.default("agency_test"),
      }),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async ({ database }) => {
      const serviceId = serviceIdForDatabase(env, database);
      const service = await migetGet(env, `/services/${serviceId}`);
      const records = safeCollection(service);
      return resultContent({
        authenticated: true,
        read_only: true,
        database,
        service: records[0] ?? {},
      });
    },
  );

  server.registerTool(
    "miget_get_banking_pay_capacity_observation",
    {
      title: "Observe Banking Pay database capacity",
      description:
        `Fixed read-only observation for ${databaseScopeDescription}. Returns the selected PostgreSQL service allocation, its resource pool's current total/used/available capacity, four fixed Miget storage/health metrics with timestamps and labels, and a bounded PostgreSQL identity/size/topology snapshot. It accepts no SQL or PromQL, returns no business rows or credentials, and preserves missing metric series as unavailable evidence rather than zero.`,
      inputSchema: z.object({
        database: allowedDatabaseTarget.default("agency_test"),
      }).strict(),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async ({ database }) => {
      const serviceId = serviceIdForDatabase(env, database);
      if (!serviceId) throw new Error(`No Miget service identity is configured for ${database}`);
      const servicePayload = await migetGet(env, `/services/${serviceId}`);
      const serviceRecord = unwrapRecord(servicePayload);
      const resourceId = typeof serviceRecord.resource_id === "string"
        ? serviceRecord.resource_id
        : String(env.MIGET_RESOURCE_ID || "");
      if (!resourceId) throw new Error(`Miget service ${serviceId} did not return a resource identity`);
      const targetIdentifiers = new Set(
        [serviceRecord.uuid, serviceRecord.id, serviceRecord.label, serviceRecord.name]
          .filter((value): value is string => typeof value === "string" && value.length > 0),
      );
      const [resourcePayload, databaseObservation, ...metricObservations] = await Promise.all([
        migetGet(env, `/resources/${resourceId}`),
        collectCapacityDatabaseObservation(env, database),
        ...BANKING_PAY_OBSERVATION_METRICS.map((metric) => observeMigetMetric(env, metric, targetIdentifiers)),
      ]);
      const resourceRecord = unwrapRecord(resourcePayload);
      return resultContent({
        observation_contract: "CLOUDTMS_BANKING_PAY_CAPACITY_OBSERVATION_V1",
        authenticated: true,
        read_only: true,
        secrets_returned: false,
        caller_supplied_sql_accepted: false,
        caller_supplied_promql_accepted: false,
        business_rows_returned: 0,
        database,
        observed_at: new Date().toISOString(),
        target_metric_identities: [...targetIdentifiers].sort(),
        service: safeScalarRecord(serviceRecord, SERVICE_CAPACITY_KEYS),
        resource: safeScalarRecord(resourceRecord, RESOURCE_CAPACITY_KEYS),
        database_observation: databaseObservation,
        metrics: metricObservations,
        metric_series_missing_is_zero: false,
        activation_evidence_complete: metricObservations.every((observation) => observation.target_series_found),
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
      const resources = safeCollection(await migetGet(
        env,
        env.MIGET_TEST_ONLY === "true" ? `/resources/${env.MIGET_RESOURCE_ID}` : "/resources",
      ));
      return resultContent({
        accepted: resources.length > 0,
        authenticated: true,
        read_only: !transactionRollbackAvailable,
        rollback_only_transaction_tool: transactionRollbackAvailable,
        durable_database_writes_allowed: false,
        route: transactionRollbackAvailable
          ? "Codex -> authenticated MCP -> Miget TEST read tools plus rollback-only transaction rehearsal"
          : "ChatGPT web -> read-only MCP -> Miget REST API",
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
        `Read-only PostgreSQL catalog inspection for ${databaseScopeDescription}. Returns database version and counts of non-system functions, triggers, RLS policies, relations, and installed extensions. It cannot execute caller-supplied SQL.`,
      inputSchema: z.object({
        database: allowedDatabaseTarget.default("agency_test"),
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
    "miget_db_relation_metadata",
    {
      title: "Inspect PostgreSQL relation metadata",
      description:
        `Read-only, caller-targeted PostgreSQL catalog inspection for named relations in ${databaseScopeDescription}. Returns one explicitly paginated metadata section at a time: relation identity/ownership, columns, triggers and their functions, rewrite rules, constraints, foreign keys, dependency edges, direct/effective privileges, RLS policies, or indexes. The indexes section supports an optional exact index_names filter so detailed native bindings can be returned without oversized unrelated index records. It returns no business rows, accepts no SQL, reports totals and EOF, and limits each request to 50 relations, 50 index names, 20 roles and 100 metadata records.`,
      inputSchema: z.object({
        database: allowedDatabaseTarget.default("agency_test"),
        relations: RELATION_METADATA_TARGETS,
        section: RELATION_METADATA_SECTION.default("relations"),
        roles: RELATION_METADATA_ROLES.default([...RELATION_METADATA_DEFAULT_ROLES]),
        index_names: RELATION_METADATA_INDEX_NAMES.optional(),
        limit: z.number().int().min(1).max(100).default(25),
        offset: z.number().int().min(0).max(100_000).default(0),
      }).strict(),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async ({ database, relations, section, roles, index_names, limit, offset }) => {
      const page = await withReadOnlyDatabase(env, database, (client) => (
        collectRelationMetadata(client, relations, roles, section, limit, offset, index_names)
      ));
      return resultContent({
        authenticated: true,
        read_only: true,
        database,
        ...page,
      });
    },
  );

  server.registerTool(
    "miget_db_search_definitions",
    {
      title: "Search installed PostgreSQL code definitions",
      description:
        "Reusable TEST-only, read-only catalog search across installed functions, procedures, window functions, views, materialized views, rules, triggers, RLS policies, indexes, constraints, column default/generated expressions, and event triggers. It performs case-insensitive literal substring matching, reports exact totals, kind totals, pagination/EOF, complete-definition SHA-256 identities and bounded excerpts. It accepts no SQL, returns no business rows, and cannot target LIVE.",
      inputSchema: z.object({
        database: TEST_DATABASE_TARGET.default("agency_test"),
        terms: DATABASE_CODE_SEARCH_TERMS,
        match_mode: z.enum(["any", "all"]).default("any"),
        schemas: DATABASE_CODE_SEARCH_SCHEMAS.optional(),
        object_kinds: DATABASE_CODE_SEARCH_KINDS.optional(),
        limit: z.number().int().min(1).max(100).default(25),
        offset: z.number().int().min(0).max(100_000).default(0),
        excerpt_characters: z.number().int().min(128).max(2_000).default(600),
      }).strict(),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async ({ database, terms, match_mode, schemas, object_kinds, limit, offset, excerpt_characters }) => {
      const page = await withReadOnlyDatabase(env, database, (client) => searchDatabaseCode(
        client,
        terms,
        schemas,
        object_kinds,
        match_mode,
        limit,
        offset,
        excerpt_characters,
      ));
      return resultContent({
        authenticated: true,
        read_only: true,
        database,
        live_access_allowed: false,
        secrets_returned: false,
        ...page,
      });
    },
  );

  server.registerTool(
    "miget_db_routine_metadata",
    {
      title: "Inspect PostgreSQL routine ownership and EXECUTE access",
      description:
        "TEST-only, read-only catalog inspection for exact named function/procedure signatures. Returns installed ownership, expanded direct-or-default EXECUTE ACL entries, schema USAGE, authoritative effective EXECUTE/callable results for named roles, and effective inherited-role paths. It accepts no SQL, returns no business rows, and supports up to 50 exact routines and 20 roles with explicit pagination.",
      inputSchema: z.object({
        database: TEST_DATABASE_TARGET.default("agency_test"),
        routines: ROUTINE_METADATA_TARGETS,
        roles: ROUTINE_METADATA_ROLES.default([...RELATION_METADATA_DEFAULT_ROLES]),
        limit: z.number().int().min(1).max(50).default(25),
        offset: z.number().int().min(0).max(100_000).default(0),
      }).strict(),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async ({ database, routines, roles, limit, offset }) => {
      const page = await withReadOnlyDatabase(env, database, (client) => (
        collectRoutineMetadata(client, routines, roles, limit, offset)
      ));
      return resultContent({
        authenticated: true,
        read_only: true,
        database,
        live_access_allowed: false,
        secrets_returned: false,
        ...page,
      });
    },
  );

  server.registerTool(
    "miget_db_list_rpcs",
    {
      title: "List and search CloudTMS Miget PostgreSQL RPCs",
      description:
        `Read-only PostgreSQL catalog inspection for ${databaseScopeDescription}. Lists or searches non-system functions and procedures with signatures, result types, languages, volatility, and SECURITY DEFINER status. Results are paginated; no caller-supplied SQL is accepted.`,
      inputSchema: z.object({
        database: allowedDatabaseTarget.default("agency_test"),
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
        `Read-only PostgreSQL catalog inspection for ${databaseScopeDescription}. Returns exact pg_get_functiondef output for a named non-system function or procedure, including overload signatures. It cannot execute the function or caller-supplied SQL.`,
      inputSchema: z.object({
        database: allowedDatabaseTarget.default("agency_test"),
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
        `Read-only inspection of the protected migration and repeatable ledgers for ${databaseScopeDescription}. Returns counts and the most recent installed paths/hashes without accepting caller-supplied SQL.`,
      inputSchema: z.object({
        database: allowedDatabaseTarget.default("agency_test"),
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
        "Read-only fixed catalog audit for RLS coverage, exposed grants, PUBLIC function execution, SECURITY DEFINER search_path configuration, and installed extensions. For agency TEST it also returns complete fixed R11 effective-access evidence for the six Banking Pay and release-control relations, including actual-role zero-row probes and anonymous zero-row PostgREST boundary probes. It accepts no caller-supplied SQL and returns no business rows or credentials.",
      inputSchema: z.object({
        database: allowedDatabaseTarget.default("agency_test"),
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
        const summary = response.rows[0] as Record<string, unknown>;
        if (database !== "agency_test") return summary;
        const catalog = await collectBankingPayR11CatalogEvidence(client);
        const roleProbes = await collectBankingPayR11RoleProbes(client);
        return {
          ...summary,
          banking_pay_r11_effective_access: {
            profile: "BANKING_PAY_R11_EFFECTIVE_ACCESS_V1",
            fixed_target_object_count: BANKING_PAY_R11_OBJECTS.length,
            fixed_target_role_count: BANKING_PAY_R11_ROLES.length,
            truncated: false,
            business_rows_returned: 0,
            caller_supplied_sql_accepted: false,
            catalog,
            role_probes: roleProbes,
          },
        };
      });
      if (database === "agency_test") {
        const effectiveAccess = audit.banking_pay_r11_effective_access as Record<string, unknown>;
        effectiveAccess.anonymous_postgrest_zero_row_probes = await probeBankingPayR11ApiBoundary(env);
      }
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
        database: allowedDatabaseTarget.default("agency_test"),
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
    if (url.pathname === "/miget-gateway/health") {
      return Response.json({
        ok: true,
        service: "cloudtms-miget-gateway",
        version: "1.0.0",
        databases: env.MIGET_TEST_ONLY === "true"
          ? ["agency_test", "mytms_test"]
          : ["agency_test", "mytms_test", "agency_live"],
        rollback_only_transaction_tool: env.MIGET_TRANSACTION_ROLLBACK_ENABLED === "true",
        durable_database_writes_allowed: false,
      });
    }
    if (url.pathname === POSTGREST_PREFIX || url.pathname.startsWith(`${POSTGREST_PREFIX}/`)) {
      if (env.MIGET_TEST_ONLY === "true") return new Response("Not found", { status: 404 });
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
      const fromChatGptWrapper = request.headers.get("x-cloudtms-internal-route") === "chatgpt-test-wrapper-v1";
      const transactionRollbackAvailable =
        env.MIGET_TRANSACTION_ROLLBACK_ENABLED === "true" && !fromChatGptWrapper;
      const handler = createMcpHandler(() => createServer(env, transactionRollbackAvailable), {
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
