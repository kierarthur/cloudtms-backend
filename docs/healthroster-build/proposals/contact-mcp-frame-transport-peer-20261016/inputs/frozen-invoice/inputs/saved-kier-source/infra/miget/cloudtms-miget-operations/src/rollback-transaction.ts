import { Client, type QueryResult } from "pg";
import { prepareSqlBatch } from "../../cloudtms-miget-transaction-lab/src/sql-envelope";

const ADVISORY_LOCK_KEYS = [1129598292, 1413565762] as const;
const MAX_CELL_TEXT = 20_000;

export interface RollbackTransactionOptions {
  databaseTarget: "agency_test" | "mytms_test";
  expectedDatabase: string;
  label: string;
  sql: string;
  verificationSql?: string;
  statementTimeoutMs: number;
  lockTimeoutMs: number;
  maxRowsPerStatement: number;
  continueOnError: boolean;
}

interface StatementOutcome {
  statement: number;
  command?: string;
  rowCount?: number | null;
  fields?: string[];
  rows?: unknown[];
  rowsTruncated?: boolean;
  error?: Record<string, unknown>;
}

export interface RollbackTransactionResult {
  accepted: boolean;
  databaseTarget: "agency_test" | "mytms_test";
  database: string;
  label: string;
  sqlSha256: string;
  statementCount: number;
  statements: StatementOutcome[];
  rolledBack: boolean;
  rollbackVerification: { connectedFresh: boolean; database: string; statements: StatementOutcome[] };
  warnings: string[];
  fatalError?: Record<string, unknown>;
}

type ClientFactory = () => Client;

function errorRecord(error: unknown): Record<string, unknown> {
  if (!(error instanceof Error)) return { message: "Unknown PostgreSQL error" };
  const source = error as Error & Record<string, unknown>;
  const output: Record<string, unknown> = { message: error.message };
  for (const key of [
    "code", "detail", "hint", "position", "where", "schema", "table", "column", "constraint", "routine",
  ]) {
    const value = source[key];
    if (typeof value === "string" && value.length > 0) output[key] = value.slice(0, MAX_CELL_TEXT);
  }
  return output;
}

function jsonValue(value: unknown, depth = 0): unknown {
  if (depth > 8) return "[depth limit]";
  if (value === null || typeof value === "boolean" || typeof value === "number") return value;
  if (typeof value === "bigint") return value.toString();
  if (typeof value === "string") return value.length <= MAX_CELL_TEXT ? value : `${value.slice(0, MAX_CELL_TEXT)}…`;
  if (value instanceof Date) return value.toISOString();
  if (value instanceof Uint8Array) return `[binary ${value.byteLength} bytes]`;
  if (Array.isArray(value)) return value.map((item) => jsonValue(item, depth + 1));
  if (typeof value === "object" && value !== null) {
    return Object.fromEntries(Object.entries(value).map(([key, child]) => [key, jsonValue(child, depth + 1)]));
  }
  return String(value);
}

function outcome(statement: number, result: QueryResult, maxRows: number): StatementOutcome {
  const rows = result.rows.slice(0, maxRows).map((row) => jsonValue(row));
  return {
    statement,
    command: result.command,
    rowCount: result.rowCount,
    fields: result.fields.map((field) => field.name),
    rows,
    rowsTruncated: result.rows.length > rows.length,
  };
}

async function sha256Hex(value: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
}

async function assertDatabase(
  client: Client,
  expectedDatabase: string,
  expectedReadOnly: "on" | "off",
): Promise<string> {
  const response = await client.query<{ database: string; in_recovery: boolean; transaction_read_only: string }>(`
    select current_database() as database,
           pg_is_in_recovery() as in_recovery,
           current_setting('transaction_read_only') as transaction_read_only
  `);
  const identity = response.rows[0];
  if (!identity || identity.database !== expectedDatabase) {
    throw new Error(`Rollback tool expected ${expectedDatabase}, received ${identity?.database ?? "unknown"}`);
  }
  if (identity.in_recovery) throw new Error("Rollback tool cannot run on a recovery replica");
  if (identity.transaction_read_only !== expectedReadOnly) {
    throw new Error(`Rollback tool expected transaction_read_only=${expectedReadOnly}, received ${identity.transaction_read_only}`);
  }
  return identity.database;
}

async function runStatements(
  client: Client,
  statements: string[],
  maxRows: number,
  continueOnError: boolean,
): Promise<{ outcomes: StatementOutcome[]; fatalError?: Record<string, unknown> }> {
  const outcomes: StatementOutcome[] = [];
  for (let index = 0; index < statements.length; index += 1) {
    const savepoint = `cloudtms_codex_rollback_statement_${index + 1}`;
    if (continueOnError) await client.query(`savepoint ${savepoint}`);
    try {
      const result = await client.query(statements[index]);
      outcomes.push(outcome(index + 1, result, maxRows));
      if (continueOnError) await client.query(`release savepoint ${savepoint}`);
    } catch (error) {
      const recorded = errorRecord(error);
      outcomes.push({ statement: index + 1, error: recorded });
      if (!continueOnError) return { outcomes, fatalError: recorded };
      await client.query(`rollback to savepoint ${savepoint}`);
      await client.query(`release savepoint ${savepoint}`);
    }
  }
  return { outcomes };
}

async function verifyAfterRollback(
  createClient: ClientFactory,
  statements: string[],
  options: RollbackTransactionOptions,
): Promise<RollbackTransactionResult["rollbackVerification"]> {
  const client = createClient();
  await client.connect();
  try {
    await client.query("begin read only");
    await client.query("select set_config('statement_timeout', $1, true)", [`${options.statementTimeoutMs}ms`]);
    await client.query("select set_config('lock_timeout', $1, true)", [`${options.lockTimeoutMs}ms`]);
    const database = await assertDatabase(client, options.expectedDatabase, "on");
    const execution = await runStatements(client, statements, options.maxRowsPerStatement, false);
    if (execution.fatalError) throw new Error("Post-rollback verification query failed");
    await client.query("rollback");
    return { connectedFresh: true, database, statements: execution.outcomes };
  } catch (error) {
    await client.query("rollback").catch(() => undefined);
    throw error;
  } finally {
    await client.end().catch(() => undefined);
  }
}

export async function runRollbackTransaction(
  createClient: ClientFactory,
  options: RollbackTransactionOptions,
): Promise<RollbackTransactionResult> {
  // The account-specific Operations worker may rehearse one complete release
  // candidate. Keep the shared transaction-lab default at 250 statements while
  // allowing this TEST-only, server-owned transaction a larger finite envelope.
  const statements = prepareSqlBatch(options.sql, 1_000);
  const verificationStatements = options.verificationSql ? prepareSqlBatch(options.verificationSql) : [];
  const sqlSha256 = await sha256Hex(options.sql);
  const client = createClient();
  let rolledBack = false;
  let database = "unknown";
  let execution: Awaited<ReturnType<typeof runStatements>> = { outcomes: [] };

  await client.connect();
  try {
    await client.query("begin");
    await client.query("select set_config('statement_timeout', $1, true)", [`${options.statementTimeoutMs}ms`]);
    await client.query("select set_config('lock_timeout', $1, true)", [`${options.lockTimeoutMs}ms`]);
    await client.query("select set_config('idle_in_transaction_session_timeout', $1, true)", [
      `${options.statementTimeoutMs + 30_000}ms`,
    ]);
    database = await assertDatabase(client, options.expectedDatabase, "off");
    const lock = await client.query<{ acquired: boolean }>(
      "select pg_try_advisory_xact_lock($1, $2) as acquired",
      [...ADVISORY_LOCK_KEYS],
    );
    if (!lock.rows[0]?.acquired) throw new Error("Another rollback rehearsal is already running on this database");
    execution = await runStatements(client, statements, options.maxRowsPerStatement, options.continueOnError);
  } catch (error) {
    if (!execution.fatalError) execution.fatalError = errorRecord(error);
  } finally {
    try {
      await client.query("rollback");
      rolledBack = true;
    } finally {
      await client.end().catch(() => undefined);
    }
  }

  if (!rolledBack) throw new Error("PostgreSQL rollback could not be confirmed");
  const rollbackVerification = await verifyAfterRollback(createClient, verificationStatements, options);
  return {
    accepted: !execution.fatalError,
    databaseTarget: options.databaseTarget,
    database,
    label: options.label,
    sqlSha256,
    statementCount: statements.length,
    statements: execution.outcomes,
    rolledBack,
    rollbackVerification,
    warnings: [
      "All transactional database and catalog changes were rolled back.",
      "PostgreSQL sequence increments (including serial/identity nextval calls) are not rolled back and can leave harmless gaps.",
      "Do not invoke functions with payment, provider, email, network, or other effects outside PostgreSQL; those effects are not covered by rollback.",
    ],
    ...(execution.fatalError ? { fatalError: execution.fatalError } : {}),
  };
}
