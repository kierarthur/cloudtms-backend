# CloudTMS Codex Operating Instructions

## Purpose

These instructions apply to Codex work on CloudTMS. They are mandatory unless the user explicitly overrides a specific instruction in the current task.

CloudTMS currently uses a frontend-primary Codex environment:

* Primary writable repository: `/workspace/TEST-Frontend`
* Backend local clone: `/workspace/cloudtms-backend`
* Backend GitHub repository is read-only from the frontend-primary Codex environment.
* Backend files may be edited locally for patch-worker testing only.
* Backend GitHub changes must be returned as patch/replacement files unless the backend repository is the primary writable repository for the current Codex task.

## CloudTMS TEST-only rule

This environment is TEST-only.

Never use production endpoints, production Supabase, production Cloudflare, production payment providers, production credentials, production Workers, production R2 buckets, production KV namespaces, production webhooks, or production databases.

Never deploy to production.

Never deploy to the normal TEST backend unless the user explicitly instructs that exact action in the current task.

The isolated Codex backend patch Worker is:

```text
https://codex-cloudtms-backend.kier-88a.workers.dev
```

The normal TEST frontend is:

```text
https://testmode.arthur-rai.co.uk
```

The normal TEST backend is:

```text
https://test-cloudtms-backend.kier-88a.workers.dev
```

The isolated Codex Worker must use:

```text
Worker: codex-cloudtms-backend
R2 bucket: test-cloudtms-preview
KV namespace: cloudtms-codex-sessions
KV namespace id: 6f3888a777f844959e35f4b2fb0dce9b
Supabase: TEST project only
Crons: disabled
```

## Secrets and sensitive data

Do not print, log, echo, expose, commit, or include in reports:

* Cloudflare tokens
* Supabase keys
* Supabase service-role keys
* DB URLs
* Passwords
* Cookies
* Access tokens
* Refresh tokens
* Auth headers
* `.dev.vars`
* Temporary secret files
* Full sensitive API payloads
* Full user records
* Full payment/banking/provider payloads
* Any credential-like value

When confirming environment variables, report names only and whether they are present. Do not print values.

## Supabase and database safety

Codex must not run destructive Supabase, SQL, RPC, or API actions unless the user explicitly instructs that exact action in the current task.

Destructive actions include, but are not limited to:

* `DROP`, `TRUNCATE`, `ALTER`, `CREATE`, `REINDEX`, schema migrations, or any other DDL
* Running migration files
* `DELETE` from application tables
* `UPDATE` or `PATCH` of broad or unbounded row sets
* `INSERT` or `UPSERT` of non-test data
* Any mutation without a specific test identifier
* Payment execution
* Provider submission
* Settlement
* Unwind
* Webhook replay
* Remittance send
* Email drain
* Comms drain
* Background worker/drain RPCs
* Any RPC that schedules payments, sends emails, touches bank/provider state, creates/voids payment batches, settles transfers, or mutates finance artifacts

For diagnostics, prefer read-only actions:

* `SELECT`-only Supabase REST requests
* `GET` requests
* Health/readiness checks
* Playwright observation
* Worker logs
* Reading source code
* Harmless RPCs only where confirmed read-only

If a test requires a write, Codex must stop and state:

1. What it wants to mutate.
2. Why the mutation is necessary.
3. Which exact TEST identifiers/rows are affected.
4. How it will verify the outcome.
5. How it will avoid broad impact.

Do not proceed with the write until the user explicitly approves it in the current task.

## SQL file naming and placement

Every new SQL file must use the filename format `DDMMYYYY_HHMM_name.sql`; the filename must always begin with the date and 24-hour time in that exact order. Use the current UK date and time, and use a short descriptive `snake_case` name.

* Save one-time database migrations in `supabase\migrations`.
* Save new or replacement SQL function definitions in `supabase\repeatable`.
* Do not save SQL functions as one-time migrations unless a separately required schema/data migration calls or installs them as part of an explicitly approved change.
* Never rename an already-applied migration merely to adopt this convention unless the user explicitly requests it and the migration history has been checked safe.

## Catalogued SQL and migration-workflow verification

Before changing or publishing any SQL function that is covered by a catalogue manifest:

* Inspect the current migration/deployment workflow and the current catalogue verifier immediately before editing. Do not rely on how an earlier workflow behaved.
* Identify the function's one authoritative manifest owner and preserve the verifier's current owner, security, configuration, ACL, signature, and source-file requirements.
* When TEST DDL validation is explicitly authorised, compile the exact staged Git blob (or committed Git blob)—not a Windows working-tree rendering—inside a transaction, obtain the definition hash from PostgreSQL's canonical `pg_get_functiondef` representation using the same algorithm as the current verifier, and roll the transaction back. The staged/committed blob is the authority because it is what the Linux workflow will install.
* Put that canonical database-derived hash in the manifest. Do not substitute a hash of saved SQL file bytes, and do not compile a CRLF working-tree copy when Git will publish LF: function-body line endings survive inside `pg_get_functiondef` and can change the verifier hash even when the SQL logic is identical.
* Do not guess a manifest hash when canonical compilation or verification is unavailable. Stop and report the missing proof.
* After publishing, require the current workflow to pass and recheck the installed TEST definition and manifest hash before declaring deployment complete.

## TEST-only diagnostic RPC

The TEST Supabase project has a diagnostic RPC:

```text
public.codex_debug_select_sql(p_sql text, p_limit integer default 100)
```

Codex may use this RPC only for TEST-only read-only diagnostics through Supabase REST:

```text
/rest/v1/rpc/codex_debug_select_sql
```

Allowed use:

* Bounded `SELECT` or `WITH` diagnostic queries
* Joins
* Grouping
* JSON inspections
* Consistency checks
* Status summaries
* Verification of specific TEST IDs

Forbidden use:

* Destructive SQL
* DDL
* Migrations
* DML
* Locks
* Transactions
* Payment execution
* Settlement
* Provider submission
* Remittance
* Webhook replay
* Email/comms drains
* Background worker/drain operations
* Returning or printing sensitive row data unless safe and necessary

Do not print service-role keys or full sensitive query results.

## CloudTMS Policy X

For Banking Pay and payment logic, Policy X is mandatory.

* Pre-draft may use live truth.
* Post-draft must use frozen batch artifacts only.
* `TS_DAY` remains date-bucketed as `YYYY-MM-DD`.
* Do not alter finance/payment/economic logic unless explicitly instructed.
* Do not introduce live finance-component identity fallback post-draft.
* Do not invent a new economic-key derivation ladder.
* Do not bypass central freshness/staleness validation.
* Do not change settlement/remittance/provider behaviour unless explicitly instructed.

Any implementation plan, SQL, backend code, frontend code, or test involving Banking Pay must explicitly avoid Policy X drift.

## Repository model

### Frontend-primary Codex environment

In the current full-stack environment:

```text
Primary writable repo: /workspace/TEST-Frontend
Backend local clone: /workspace/cloudtms-backend
```

Codex may:

* Modify frontend files in `/workspace/TEST-Frontend`.
* Read backend files in `/workspace/cloudtms-backend`.
* Locally patch backend files for isolated testing.
* Deploy the locally patched backend clone to the isolated Codex Worker.
* Route Playwright requests from the TEST frontend to the isolated Codex Worker.
* Return backend patches/replacement files in the frontend repo under `codex_outputs/`.

Codex must not:

* Push backend changes from the frontend-primary environment.
* Commit backend changes from the frontend-primary environment.
* Modify backend `wrangler.toml`.
* Deploy the normal TEST backend.
* Deploy production.

### Backend-primary Codex environment

If a future Codex task uses the backend repository as the primary writable repo, backend commits/PRs may be produced only if the user asks for them. All safety rules in this file still apply.

## Backend patch-worker workflow

When testing backend changes from the frontend-primary environment, use the isolated patch-worker workflow.

Do not modify:

```text
/workspace/cloudtms-backend/wrangler.toml
```

Instead, generate a temporary Wrangler config outside the repository:

```text
/tmp/cloudtms-codex-wrangler.toml
```

The temporary config must deploy only:

```text
codex-cloudtms-backend
```

It must point to:

```text
/workspace/cloudtms-backend/broker/src/index.js
```

Use:

```text
R2 bucket: test-cloudtms-preview
KV binding: SESSIONS
KV namespace id: 6f3888a777f844959e35f4b2fb0dce9b
Supabase URL: TEST Supabase only
Crons: disabled
```

Use temporary secrets outside the repo:

```text
/tmp/cloudtms-codex-worker-secrets.env
```

The current Wrangler version in Codex rejected `deploy --secrets-file`, so use this proven pattern:

```bash
cd /workspace/cloudtms-backend
npx wrangler deploy --config /tmp/cloudtms-codex-wrangler.toml
npx wrangler secret bulk /tmp/cloudtms-codex-worker-secrets.env --config /tmp/cloudtms-codex-wrangler.toml
```

Delete temporary config and secret files before finishing:

```bash
rm -f /tmp/cloudtms-codex-wrangler.toml
rm -f /tmp/cloudtms-codex-worker-secrets.env
```

After testing, restore backend tracked files unless the user explicitly asks to keep local backend changes:

```bash
cd /workspace/cloudtms-backend
git checkout -- <changed tracked backend files>
```

Confirm:

* Backend `wrangler.toml` was not modified.
* Backend tracked files were restored or intentionally output as patches.
* No deploy was made to normal TEST.
* No deploy was made to production.

## Playwright routing workflow

When testing backend patches against the TEST frontend:

1. Open the normal TEST frontend:

```text
https://testmode.arthur-rai.co.uk
```

2. Install Playwright routing before navigation.

3. Rewrite requests beginning with:

```text
https://test-cloudtms-backend.kier-88a.workers.dev
```

to:

```text
https://codex-cloudtms-backend.kier-88a.workers.dev
```

4. Preserve path, query string, method, headers, and body.

5. Verify:

* Backend requests were intercepted.
* Requests hit the Codex backend Worker.
* No backend requests escaped to the normal TEST backend.
* Login works through the routed Codex backend.
* `/api/me` or equivalent authenticated current-user check works through the routed Codex backend.

Do not print response bodies, cookies, tokens, user records, or sensitive payloads.

## Standard output requirements for backend changes

Because backend GitHub is read-only in the frontend-primary environment, backend changes must be returned under:

```text
codex_outputs/
```

Use this structure where relevant:

```text
codex_outputs/implementation_plan.md
codex_outputs/backend_patch.diff
codex_outputs/backend_replacement_files/
codex_outputs/test_report.md
```

For backend replacement files/functions:

* Include only files/functions that actually changed.
* Do not include unchanged functions.
* Provide full replacement function code for changed functions.
* Do not use placeholders.
* Do not omit code for brevity.
* Provide a diff.
* Provide a detailed implementation plan.
* Explain tests run and results.
* Confirm Policy X compliance where Banking Pay/payment logic is involved.

## Frontend code changes

When modifying the frontend:

* Keep changes narrowly scoped to the user’s request.
* Preserve existing UI date/time format: `DD/MM/YYYY hh:mm:ss`, 24-hour clock.
* Do not alter unrelated modal framework, auth/session handling, Banking Pay, payment, settlement, remittance, webhook, or finance logic unless explicitly asked.
* If a change affects backend interaction, verify via Playwright against the Codex backend route where appropriate.

## Database/schema verification before SQL or code

Before providing SQL, RPC code, or backend code that references database objects, verify all referenced tables, columns, enums, and functions against the current schema/source available in the workspace or user-provided files.

Do not assume schema objects exist.

If schema verification is incomplete, state exactly what could not be verified.

## Forbidden shortcuts

Do not:

* Say a task is complete unless it was actually tested or clearly marked as untested.
* Invent function names, table names, or column names.
* Use old code versions when the user has posted a newer version in the current task.
* Return placeholder code.
* Return partial functions when the user asks for full functions.
* Broaden a highly targeted fix.
* Modify unrelated systems.
* Run `npm audit fix` or dependency upgrades unless explicitly asked.
* Run migrations unless explicitly asked.
* Run payment/remittance/settlement/webhook/email/background drains unless explicitly asked and safe.

## Final report expectations

Every Codex task should return a concise but complete report containing:

* Files changed.
* Backend files changed locally, if any.
* Whether backend changes were restored.
* Whether backend replacement files/diffs were created.
* Tests run.
* Test results.
* Whether the Codex backend Worker was deployed.
* Whether Playwright routed requests to the Codex backend.
* Whether any requests escaped to normal TEST backend.
* Confirmation no TEST/PROD deploy occurred unless explicitly requested.
* Confirmation no destructive SQL or prohibited RPCs were run.
* Confirmation secrets were not printed.
