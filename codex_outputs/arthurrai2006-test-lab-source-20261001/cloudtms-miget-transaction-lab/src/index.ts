import { McpServer } from "@modelcontextprotocol/server";
import { createMcpHandler } from "agents/mcp/server";
import { timingSafeEqual } from "node:crypto";
import { Client } from "pg";
import { z } from "zod";
import { runRollbackTransaction } from "./transaction-runner.ts";

const ROUTE = "/mcp";
const WRITE_ANNOTATIONS = {
  readOnlyHint: false,
  destructiveHint: true,
  idempotentHint: false,
  openWorldHint: false,
} as const;

function authorized(request: Request, env: Env): boolean {
  const expected = String(env.MIGET_TRANSACTION_LAB_ROUTE_TOKEN || "");
  const url = new URL(request.url);
  const bearer = request.headers.get("authorization")?.match(/^Bearer\s+(.+)$/i)?.[1] ?? "";
  const supplied = bearer || url.searchParams.get("access_token") || "";
  if (!expected || supplied.length !== expected.length) return false;
  return timingSafeEqual(Buffer.from(supplied), Buffer.from(expected));
}

function resultContent(result: Record<string, unknown>) {
  return {
    content: [{ type: "text" as const, text: JSON.stringify(result, null, 2) }],
    structuredContent: result,
  };
}

function createServer(env: Env): McpServer {
  const server = new McpServer({
    name: "CloudTMS Miget Transaction Lab",
    version: "0.1.0",
  });

  server.registerTool(
    "miget_db_transaction_rollback",
    {
      title: "Run SQL/RPC variations and always roll back",
      description:
        "TEST-only PostgreSQL transaction lab. Runs a broad multi-statement SQL batch—including DDL, DML, CREATE OR REPLACE FUNCTION, DO blocks, procedure calls, and RPC calls—on the CloudTMS Miget TEST database using one connection and one server-owned transaction, then unconditionally rolls it back. Optional verification SQL runs afterward on a fresh read-only connection. Only outer transaction escape commands are rejected.",
      inputSchema: z.object({
        label: z.string().trim().min(1).max(120),
        sql: z.string().min(1).max(2_000_000),
        verification_sql: z.string().min(1).max(1_000_000).optional(),
        statement_timeout_ms: z.number().int().min(100).max(120_000).default(30_000),
        lock_timeout_ms: z.number().int().min(100).max(10_000).default(2_000),
        max_rows_per_statement: z.number().int().min(0).max(100).default(25),
        continue_on_error: z.boolean().default(false),
      }),
      annotations: WRITE_ANNOTATIONS,
    },
    async ({
      label,
      sql,
      verification_sql,
      statement_timeout_ms,
      lock_timeout_ms,
      max_rows_per_statement,
      continue_on_error,
    }) => {
      const startedAt = Date.now();
      const result = await runRollbackTransaction(
        () =>
          new Client({
            connectionString: env.HYPERDRIVE.connectionString,
            application_name: "cloudtms-miget-transaction-lab",
            connectionTimeoutMillis: 10_000,
            query_timeout: statement_timeout_ms + 5_000,
          }),
        {
          label,
          sql,
          verificationSql: verification_sql,
          statementTimeoutMs: statement_timeout_ms,
          lockTimeoutMs: lock_timeout_ms,
          maxRowsPerStatement: max_rows_per_statement,
          continueOnError: continue_on_error,
        },
      );
      console.log(
        JSON.stringify({
          message: "Miget TEST transaction rehearsal completed",
          label,
          sql_sha256: result.sqlSha256,
          statement_count: result.statementCount,
          accepted: result.accepted,
          rolled_back: result.rolledBack,
          elapsed_ms: Date.now() - startedAt,
        }),
      );
      return resultContent(result as unknown as Record<string, unknown>);
    },
  );

  return server;
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/health") {
      return Response.json({
        ok: true,
        service: "codex-cloudtms-miget-transaction-lab",
        version: "0.1.0",
        database: "agency_test",
        rollback_only: true,
      });
    }
    if (url.pathname !== ROUTE) return new Response("Not found", { status: 404 });
    if (!authorized(request, env)) {
      return Response.json(
        { error: "CloudTMS transaction-lab authorization required" },
        { status: 401, headers: { "cache-control": "no-store" } },
      );
    }

    try {
      const handler = createMcpHandler(() => createServer(env), {
        route: ROUTE,
        legacy: "stateless",
        responseMode: "auto",
        onerror(error) {
          console.error(JSON.stringify({ message: "Transaction-lab MCP request failed", error: error.message }));
        },
      });
      return await handler(request, env, ctx);
    } catch (error) {
      console.error(
        JSON.stringify({
          message: "Transaction-lab MCP bridge failed",
          error: error instanceof Error ? error.message : "Unknown error",
        }),
      );
      return Response.json({ error: "Transaction-lab MCP bridge failed" }, { status: 500 });
    }
  },
} satisfies ExportedHandler<Env>;
