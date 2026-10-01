import { Client, type QueryResult } from "pg";
import { prepareSqlBatch } from "./sql-envelope.ts";

const EXPECTED_DATABASE = "cloudtms_test_clone";
const ADVISORY_LOCK_KEYS = [1129598292, 1413565762] as const;
const MAX_CELL_TEXT = 20_000;

export interface TransactionLabOptions {
  label: string;
  sql: string;
  verificationSql?: string;
  statementTimeoutMs: number;
  lockTimeoutMs: number;
  maxRowsPerStatement: number;
  continueOnError: boolean;
}

export interface StatementOutcome {
  statement: number;
  command?: string;
  rowCount?: number | null;
  fields?: string[];
  rows?: unknown[];
  rowsTruncated?: boolean;
  error?: Record<string, unknown>;
}

export interface TransactionLabResult {
  accepted: boolean;
  database: string;
  label: string;
  sqlSha256: string;
  statementCount: number;
  statements: StatementOutcome[];
  rolledBack: boolean;
  rollbackVerification: {
    connectedFresh: boolean;
    database: string;
    statements: StatementOutcome[];
  };
  warnings: string[];
  fatalError?: Record<string, unknown>;
}

export type ClientFactory = () => Client;

function errorRecord(error: unknown): Record<string, unknown> {
  if (!(error instanceof Error)) return { message: "Unknown PostgreSQL error" };
  const source = error as Error & Record<string, unknown>;
  const output: Record<string, unknown> = { message: error.message };
  for (const key of [
    "code",
    "detail",
    "hint",
    "position",
    "where",
    "schema",
    "table",
    "column",
    "constraint",
    "routine",
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
  if (typeof value === "string") {
    return value.length <= MAX_CELL_TEXT ? value : `${value.slice(0, MAX_CELL_TEXT)}…`;
  }
  if (value instanceof Date) return value.toISOString();
  if (value instanceof Uint8Array) return `[binary ${value.byteLength} bytes]`;
  if (Array.isArray(value)) return value.map((item) => jsonValue(item, depth + 1));
  if (typeof value === "object" && value !== null) {
    return Object.fromEntries(
      Object.entries(value).map(([key, child]) => [key, jsonValue(child, depth + 1)]),
    );
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

async function assertTestDatabase(client: Client, expectedReadOnly: "on" | "off"): Promise<string> {
  const response = await client.query<{
    database: string;
    in_recovery: boolean;
    transaction_read_only: string;
  }>(`
    select
      current_database() as database,
      pg_is_in_recovery() as in_recovery,
      current_setting('transaction_read_only') as transaction_read_only
  `);
  const identity = response.rows[0];
  if (!identity || identity.database !== EXPECTED_DATABASE) {
    throw new Error(`Transaction lab expected ${EXPECTED_DATABASE}, received ${identity?.database ?? "unknown"}`);
  }
  if (identity.in_recovery) throw new Error("Transaction lab cannot run on a recovery replica");
  if (identity.transaction_read_only !== expectedReadOnly) {
    throw new Error(
      `Transaction lab expected transaction_read_only=${expectedReadOnly}, received ${identity.transaction_read_only}`,
    );
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
    const savepoint = `cloudtms_lab_statement_${index + 1}`;
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
  verificationStatements: string[],
  options: TransactionLabOptions,
): Promise<TransactionLabResult["rollbackVerification"]> {
  const client = createClient();
  await client.connect();
  try {
    await client.query("begin read only");
    await client.query("select set_config('statement_timeout', $1, true)", [
      `${options.statementTimeoutMs}ms`,
    ]);
    await client.query("select set_config('lock_timeout', $1, true)", [`${options.lockTimeoutMs}ms`]);
    const database = await assertTestDatabase(client, "on");
    const execution = await runStatements(
      client,
      verificationStatements,
      options.maxRowsPerStatement,
      false,
    );
    if (execution.fatalError) {
      throw Object.assign(new Error("Post-rollback verification query failed"), {
        cause: execution.fatalError,
      });
    }
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
  options: TransactionLabOptions,
): Promise<TransactionLabResult> {
  const statements = prepareSqlBatch(options.sql);
  const verificationStatements = options.verificationSql
    ? prepareSqlBatch(options.verificationSql)
    : [];
  const sqlSha256 = await sha256Hex(options.sql);
  const client = createClient();
  let rolledBack = false;
  let database = "unknown";
  let execution: Awaited<ReturnType<typeof runStatements>> = { outcomes: [] };

  await client.connect();
  try {
    await client.query("begin");
    await client.query("select set_config('statement_timeout', $1, true)", [
      `${options.statementTimeoutMs}ms`,
    ]);
    await client.query("select set_config('lock_timeout', $1, true)", [`${options.lockTimeoutMs}ms`]);
    await client.query("select set_config('idle_in_transaction_session_timeout', $1, true)", [
      `${options.statementTimeoutMs + 30_000}ms`,
    ]);
    database = await assertTestDatabase(client, "off");
    const lock = await client.query<{ acquired: boolean }>(
      "select pg_try_advisory_xact_lock($1, $2) as acquired",
      [...ADVISORY_LOCK_KEYS],
    );
    if (!lock.rows[0]?.acquired) throw new Error("Another transaction-lab rehearsal is already running");
    execution = await runStatements(
      client,
      statements,
      options.maxRowsPerStatement,
      options.continueOnError,
    );
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
  const rollbackVerification = await verifyAfterRollback(
    createClient,
    verificationStatements,
    options,
  );
  return {
    accepted: !execution.fatalError,
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
      "Effects outside PostgreSQL, if invoked by a database extension or privileged function, are outside transaction rollback semantics.",
    ],
    ...(execution.fatalError ? { fatalError: execution.fatalError } : {}),
  };
}
