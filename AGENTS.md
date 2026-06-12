# CloudTMS Codex Safety Rules

This environment is TEST-only. Never use production endpoints, production Supabase, production Cloudflare, production payment providers, or production credentials.

## Supabase and database safety

Codex must not run destructive Supabase, SQL, RPC, or API actions unless the user explicitly instructs that exact action in the current task.

Destructive actions include, but are not limited to:

- DROP, TRUNCATE, ALTER, CREATE, REINDEX, schema migrations, or DDL
- DELETE from application tables
- UPDATE/PATCH of broad or unbounded row sets
- INSERT/UPSERT of non-test data
- Running migration files
- Running payment execution, provider submission, settlement, unwind, webhook replay, remittance send, email drain, comms drain, or background worker/drain RPCs
- Running any RPC that schedules payments, sends emails, touches bank/provider state, creates/voids payment batches, settles transfers, or mutates finance artifacts
- Any query or script that does not include a specific test identifier when mutating data

For diagnostics, prefer read-only actions:

- SELECT-only Supabase REST requests
- GET requests
- health/readiness checks
- Playwright observation
- Worker logs
- reading source code
- harmless RPCs only where confirmed read-only

If a test requires a write, Codex must stop and state:
1. what it wants to mutate,
2. why it is necessary,
3. which exact test IDs/rows are affected,
4. how it will verify and avoid broad impact.

Codex must not print secrets, tokens, passwords, service-role keys, DB URLs, cookies, auth tokens, `.dev.vars`, or full sensitive payloads.

## CloudTMS Policy X

For Banking Pay/payment logic, Policy X is mandatory:
- Pre-draft may use live truth.
- Post-draft must use frozen batch artifacts only.
- TS_DAY remains date-bucketed as YYYY-MM-DD.
- Do not alter finance/payment/economic logic unless explicitly instructed.
