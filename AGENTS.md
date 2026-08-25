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
Database: Miget TEST through `codex-cloudtms-miget-gateway` only
Crons: disabled
```

Normal TEST and the isolated patch Worker must not route database traffic to the legacy Supabase TEST project. The logical environment names `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` may remain for compatibility, but their TEST values must target the Miget gateway and matching PostgREST JWT.

For every Miget CloudTMS/MyTMS PostgREST app, append `options=-c%20pg_show_plans.is_enabled%3Doff` to `PGRST_DB_URI`, preserving all other URI components and credentials. Miget currently preloads `pg_show_plans`; leaving it enabled caused repeated `not enough memory to append new query plans` warnings and multi-second complex-RPC latency. Redeploy and prove a new PostgREST session has the collector disabled, the warning flood is absent, and an exact real RPC passes a timing benchmark. After any resource resize, independently verify the live limits plus PostgreSQL memory settings instead of trusting the control-plane allocation alone.

Keep the agency TEST PostgREST app `01a02ff2-4d37-77f8-b440-a20655129ee1` at no less than its proven 512 MiB RAM / 0.2 CPU allocation unless a separately approved concurrency benchmark proves a replacement. A Miget app resize can restart or redeploy the app and interrupt login while PostgREST reconnects, even when the control plane initially reports `running`. Never fund CloudBeaver or another administration tool by downsizing an operational PostgreSQL, PostgREST, Worker-facing gateway, or control-plane app. Reserve free pooled capacity first, apply one change at a time, and prove direct PostgREST health plus the normal Worker login route after each allocation change.

A restored Supabase dump can retain `postgres` as the repository/logical owner while Miget provisions a generated service owner. Protected releases must use the repository's bounded logical-owner adapter to map only that audited owner to `CURRENT_USER`; never assume `SET ROLE postgres` will work. Reapply and verify the audited owner default privileges after restore because `pg_dump`/`pg_restore` do not reliably recreate provider-owner defaults for the new owner. Fail closed on every unexpected owner/grantee, and recheck PostgREST browser-role exposure after the final repeatables are installed.

Supabase's `service_role` bypassed RLS at the PostgreSQL-role level; Miget's provider-neutral `service_role` deliberately does not. Every RLS-enabled public table exposed to the Worker therefore requires the repository policy `cloudtms_miget_service_owner_all` for `CURRENT_USER, service_role` with `USING (true)` and `WITH CHECK (true)`, while ordinary table grants remain the separate access boundary. Migration `24082026_1519_miget_service_role_rls_compatibility.sql` installs and verifies that policy for restored/current tables. Every later migration that enables RLS on a new public table must create and verify the same policy in that migration; otherwise PostgREST can return an empty result and the Worker can misreport a valid login as `Invalid credentials`.

Miget/provider-owner ACL baselines can leave a public `SECURITY INVOKER` RPC executable by `service_role` while stripping EXECUTE from a nested private helper. The normal scheduler then fires successfully but the RPC fails before leasing a row. Every NEW/UPGRADE release must run role-specific private-helper verifiers after final repeatables; the email outbox requires `24082026_1721_miget_private_helper_service_acl_verification.sql`. Miget's generated owner cannot `SET ROLE service_role`, so release verification must use effective privilege catalog checks and runtime PostgREST/cron proof. Do not broaden the repair to `anon` or `authenticated`.

The application gateways are repository-controlled under `infra/miget`: agency TEST plus the read-only MCP use `cloudtms-miget-operations`, MyTMS uses `cloudtms-mytms-postgrest-gateway`, and LIVE uses `cloudtms-live-postgrest-gateway`. Never leave an operational gateway available only in `.codex-tmp` or a dashboard-created deployment.

The permanent read-only auditor is the connected ChatGPT custom connector **CloudTMS Miget Operations**, backed by `infra/miget/cloudtms-miget-operations` and the Cloudflare Worker `codex-cloudtms-miget-gateway`. New auditors must use `agency_test` for the CloudTMS TEST database, `mytms_test` for the MyTMS control database, and `agency_live` only when the user has explicitly placed a LIVE read-only audit in scope. Run `miget_verify_codex_parity_route`, `miget_list_infrastructure`, `miget_inspect_postgres`, `miget_db_catalog_summary`, `miget_db_release_ledger`, `miget_db_security_audit`, `miget_db_performance_summary`, `miget_db_list_rpcs`, and `miget_db_get_rpc_definition`. These are fixed read-only operations; never give an auditor a route that accepts arbitrary SQL. Anonymous MCP access must fail with `401`, and no tool may return a token, password, connection string, raw query text, or sensitive row payload.

An independent ChatGPT audit must first read the applicable `AGENTS.md` files from all three named repositories; ordinary ChatGPT web must not assume GitHub access loads them automatically. Before substantive audit work, the same task that will issue the verdict must then see all nine named tools, pass `miget_verify_codex_parity_route`, and inspect every database target placed in scope. If discovery or a just-discovered call fails, retry discovery/parity at most three times in that task, reselecting/reconnecting when available. After three failures, stop early with the exact tool/error evidence and ask the user for a fresh audit chat; do not continue the audit, score an application/database defect, or substitute sealed historical observations for the required fresh calls.

Current Miget identities are: project `01a02ef7-18d1-7a96-9ea2-63df1bf06adc`; pooled resource `migetuq4` / `01a02ef7-1977-79bd-ad56-7e86927d5f81`; agency TEST PostgreSQL `01a02f5a-2bee-7db2-910d-a7e71f11ba0a` / database `cloudtms_test_clone`; agency TEST PostgREST `01a02ff2-4d37-77f8-b440-a20655129ee1`; MyTMS PostgreSQL `01a03045-5d5a-7892-b555-704ba6edc733` / database `uofvkfi5`; MyTMS PostgREST `01a0306a-90cd-7bbf-80bc-8fa77c5486f1`; agency LIVE PostgreSQL `01a03205-8273-7159-952e-47029c786995`; agency LIVE PostgREST `01a032ef-3726-771b-85d2-1c6f836745e4`; Hyperdrive `11c78f14afea494c9d5e8d8ad57d41a2` (agency TEST), `7e979a8127c84c319dfc2ecf488aa903` (MyTMS), and `19fc41d1b2e34b9b8a97b4961f6e6fb5` (agency LIVE read-only connector). Reverify state through Miget rather than treating this list as a substitute for runtime evidence.

For installed-source proof, the agency database uses `private.cloudtms_migration_ledger` and `private.cloudtms_repeatable_ledger`; MyTMS uses `public.schema_migrations` and `public.schema_repeatables`. The MCP ledger tool branches by database target. The authoritative agency mutation route is the protected manual `.github/workflows/database-release.yml`, backed by `scripts/cloudtms-db-release.mjs`; normal push workflows are source checks only. Current-runtime inspection must not be redirected to either former Supabase project.

The permanent credential-protected browser database tool is pgAdmin 4 at `https://cloudtms-database-browser-gimgy.eu-east-1.migetapp.com/browser/`, Miget app `01a0339e-8a1b-76db-9d69-142c2a15e0ad`, with a 512 MiB RAM / 0.1 pooled CPU limit. It pre-registers agency TEST, MyTMS control plane and agency LIVE from declarative server JSON and obtains database passwords through protected in-container `PasswordExecCommand` files. The pgAdmin username is `pgadmin@arthur-rai.co.uk`; its generated password is stored only in ignored `.codex-tmp/pgadmin-login.secret`. Never print, commit, document or copy that password. After any restart or server-registration change, prove all three entries and run a harmless read-only `select current_database(), current_user;` check.

Secondary Cloudflare Workers use prepared dedicated `deploy/cloudflare/<worker>` branches instead of sharing the normal TEST or LIVE push branch. This includes Candidate private/synthetic/broker, both invoice Workers, all three Miget gateways, `cloudtms-local`, `codex-cloudtms-backend`, and the MyTMS manager-review Worker in its own repository. A branch alone is not a Workers Builds connection: verify the exact repository, branch, root, deploy command and managed build-token identity before relying on it. Preserve Candidate deployment order: normal backend, private, synthetic, broker last.

### Safe UPGRADE and NEW-database requests

Treat `CLOUDTMS PLAN UPGRADE <exact target>` and `CLOUDTMS PLAN NEW DATABASE <agency>` as the canonical requests for the Miget installation/release system. Both are read-only planning requests: inspect the exact target, report the precise pending migrations and new/changed repeatables plus required PostgREST, gateway, Worker and configuration work, and stop before mutation. A vague request containing “upgrade”, “new database”, “prepare”, or “what is missing” never authorises APPLY, provisioning, deployment, paid-resource creation, secret changes, feature activation or resource reallocation.

Current LIVE's first deliberate schema promotion uses `LEGACY_UPGRADE`; moving its hosting to Miget did not perform that upgrade. A managed database uses `UPGRADE`, and a proved-blank database uses `NEW`. APPLY requires the user to name the exact target and previously reviewed locked plan in the current task, explicitly authorise APPLY, and provide the protected workflow's exact commit-bound approval phrase. Never extend an APPLY to another database or operational stage by inference.

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

### Mandatory database release process

Before any database, migration, RPC, view, trigger, RLS, grant, or Supabase schema work, read and follow `docs/DATABASE_RELEASE_BIBLE.md`. Run `npm run db:check` before editing and again before handoff or publication. Use `npm run db:new:migration -- --name=<name>` for new one-time changes and `npm run db:new:repeatable -- --name=<name>` for replacement function/view authority. After a new migration is complete and reviewed, append its immutable hash with `npm run db:lock:update`; that command must refuse to re-lock changed or missing older migrations. Never edit an older one-time migration or manually repair release ledgers/hashes. Push workflows verify source only; database changes use the manual protected `Database Release` PLAN/APPLY workflow. This rule applies to every future chat and agent.

The Candidate/MyTMS boundary in `supabase/release/protected-boundary-lock.json` is frozen. If proposed work needs to alter one of those files or named objects, stop and coordinate before editing. All Banking Pay changes must also preserve Policy X.

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

The Miget TEST clone has a provider-neutral diagnostic RPC:

```text
public.codex_debug_select_sql(p_sql text, p_limit integer default 100)
```

Codex may use this RPC only for TEST-only read-only diagnostics through the Miget compatibility gateway/PostgREST:

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

### Normal TEST Wrangler target

When the user explicitly authorises deployment of the normal TEST Worker from
this repository, always use:

```powershell
npx wrangler deploy --env test
```

Never use a bare `npx wrangler deploy` for normal TEST: the top-level Wrangler
configuration targets `cloudtms-local`, not `test-cloudtms-backend`. Before the
deploy, verify that `[env.test]` still names `test-cloudtms-backend`; after the
deploy, require the Wrangler result to say `Uploaded test-cloudtms-backend` and
to return the normal TEST URL. If either proof is missing, stop and report the
wrong-target risk instead of treating the deployment as successful.

It must point to:

```text
/workspace/cloudtms-backend/broker/src/index.js
```

Use:

```text
R2 bucket: test-cloudtms-preview
KV binding: SESSIONS
KV namespace id: 6f3888a777f844959e35f4b2fb0dce9b
Logical Supabase URL: Miget TEST gateway only
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
