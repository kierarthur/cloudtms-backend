# CloudTMS Miget Transaction Lab

Repository source for the private TEST-only remote MCP Worker `codex-cloudtms-miget-transaction-lab`.

The Worker exposes one broad tool, `miget_db_transaction_rollback`. It runs multi-statement PostgreSQL SQL on one Hyperdrive client inside one Worker-owned outer transaction and always issues `ROLLBACK`. This supports temporary `CREATE OR REPLACE FUNCTION` variations, DDL, DML, `DO` blocks, procedures, RPC calls, and optional post-rollback verification on a fresh read-only connection.

Only commands that could end or detach the outer transaction are blocked. Ordinary SQL is left to PostgreSQL permissions and transaction rules. PostgreSQL sequence increments are not rolled back, so rehearsals using serial/identity columns may leave harmless gaps. Database extensions or privileged functions that cause effects outside PostgreSQL are also outside rollback semantics.

The Worker contains only the agency TEST Hyperdrive binding. It has no LIVE or MyTMS binding and no PostgREST proxy. The permanent `CloudTMS Miget Operations` auditor remains unchanged and read-only.

Never store the route token or a PostgreSQL URL here. The deployed Worker requires the secret `MIGET_TRANSACTION_LAB_ROUTE_TOKEN`.

Run `npm ci`, `npm run check`, `npm test`, and `npm run deploy:dry`. Set `TRANSACTION_LAB_TEST_DATABASE_URL` only for the disposable PostgreSQL integration tests. A real deployment is a separate TEST infrastructure action.
