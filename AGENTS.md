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

Installing a new or changed PostgREST-callable function in PostgreSQL is not sufficient runtime proof: PostgREST can retain an older schema cache and return `PGRST202` even while `pg_get_functiondef` shows the exact current function. The latest controlling repeatable for every newly added or signature-changed PostgREST RPC must issue `notify pgrst, 'reload schema';` after the definitions and grants are complete; transactional delivery occurs only after commit. Protected release verification must then call the exact RPC through the current Miget/PostgREST gateway with the intended authenticated role. A catalogue query, function hash or direct SQL invocation cannot substitute for that API-path proof. Application code must map a residual `PGRST202` to a truthful retryable dependency error, never to a false “settings were not changed” or business-validation message.

The application gateways are repository-controlled under `infra/miget`: agency TEST plus the read-only MCP use `cloudtms-miget-operations`, MyTMS uses `cloudtms-mytms-postgrest-gateway`, and LIVE uses `cloudtms-live-postgrest-gateway`. Never leave an operational gateway available only in `.codex-tmp` or a dashboard-created deployment.

The permanent read-only auditor is the connected ChatGPT custom connector **CloudTMS Miget Operations**, backed by `infra/miget/cloudtms-miget-operations` and the Cloudflare Worker `codex-cloudtms-miget-gateway`. New auditors must use `agency_test` for the CloudTMS TEST database, `mytms_test` for the MyTMS control database, and `agency_live` only when the user has explicitly placed a LIVE read-only audit in scope. Run `miget_verify_codex_parity_route`, `miget_list_infrastructure`, `miget_inspect_postgres`, `miget_db_catalog_summary`, `miget_db_release_ledger`, `miget_db_security_audit`, `miget_db_performance_summary`, `miget_db_list_rpcs`, and `miget_db_get_rpc_definition`. These are fixed read-only operations; never give an auditor a route that accepts arbitrary SQL. Anonymous MCP access must fail with `401`, and no tool may return a token, password, connection string, raw query text, or sensitive row payload.

An independent ChatGPT audit must first read the applicable `AGENTS.md` files from all three named repositories; ordinary ChatGPT web must not assume GitHub access loads them automatically. Before substantive audit work, the same task that will issue the verdict must then see all nine named tools, pass `miget_verify_codex_parity_route`, and inspect every database target placed in scope. If discovery or a just-discovered call fails, retry discovery/parity at most three times in that task, reselecting/reconnecting when available. After three failures, stop early with the exact tool/error evidence and ask the user for a fresh audit chat; do not continue the audit, score an application/database defect, or substitute sealed historical observations for the required fresh calls.

Current Miget identities are: project `01a02ef7-18d1-7a96-9ea2-63df1bf06adc`; pooled resource `migetuq4` / `01a02ef7-1977-79bd-ad56-7e86927d5f81`; agency TEST PostgreSQL `01a02f5a-2bee-7db2-910d-a7e71f11ba0a` / database `cloudtms_test_clone`; agency TEST PostgREST `01a02ff2-4d37-77f8-b440-a20655129ee1`; MyTMS PostgreSQL `01a03045-5d5a-7892-b555-704ba6edc733` / database `uofvkfi5`; MyTMS PostgREST `01a0306a-90cd-7bbf-80bc-8fa77c5486f1`; agency LIVE PostgreSQL `01a03205-8273-7159-952e-47029c786995`; agency LIVE PostgREST `01a032ef-3726-771b-85d2-1c6f836745e4`; Hyperdrive `11c78f14afea494c9d5e8d8ad57d41a2` (agency TEST), `7e979a8127c84c319dfc2ecf488aa903` (MyTMS), and `19fc41d1b2e34b9b8a97b4961f6e6fb5` (agency LIVE read-only connector). Reverify state through Miget rather than treating this list as a substitute for runtime evidence.

For installed-source proof, the agency database uses `private.cloudtms_migration_ledger` and `private.cloudtms_repeatable_ledger`; MyTMS uses `public.schema_migrations` and `public.schema_repeatables`. The MCP ledger tool branches by database target. The authoritative agency mutation route is the protected manual `.github/workflows/database-release.yml`, backed by `scripts/cloudtms-db-release.mjs`; normal push workflows are source checks only. Current-runtime inspection must not be redirected to either former Supabase project.

The permanent credential-protected browser database tool is pgAdmin 4 at `https://cloudtms-database-browser-gimgy.eu-east-1.migetapp.com/browser/`, Miget app `01a0339e-8a1b-76db-9d69-142c2a15e0ad`, with a 512 MiB RAM / 0.1 pooled CPU limit. It pre-registers agency TEST, MyTMS control plane and agency LIVE from declarative server JSON and obtains database passwords through protected in-container `PasswordExecCommand` files. The pgAdmin username is `pgadmin@arthur-rai.co.uk`; its generated password is stored only in ignored `.codex-tmp/pgadmin-login.secret`. Never print, commit, document or copy that password. After any restart or server-registration change, prove all three entries and run a harmless read-only `select current_database(), current_user;` check.

Secondary Cloudflare Workers use prepared dedicated `deploy/cloudflare/<worker>` branches instead of sharing the normal TEST or LIVE push branch. This includes Candidate private/synthetic/broker, both invoice Workers, all three Miget gateways, `cloudtms-local`, `codex-cloudtms-backend`, and the MyTMS manager-review Worker in its own repository. A branch alone is not a Workers Builds connection: verify the exact repository, branch, root, deploy command and managed build-token identity before relying on it. Preserve Candidate deployment order: normal backend, private, synthetic, broker last.

The established Git-connected synthetic Candidate build invokes `candidate-private-api/wrangler.synthetic.jsonc`. That tracked compatibility config must remain semantically identical to `candidate-synthetic-private-api/wrangler.jsonc` except for its relative `main` path; `tests/candidate-broker-boundary.test.js` enforces this. Do not remove it or point the trigger at an untracked filename. A successful local deployment does not excuse a failing Git trigger.

The public Candidate broker's invitation/account journey requires two distinct versioned secret authorities in addition to the ordinary access/refresh/session authorities: `MYTMS_GLOBAL_CHALLENGE_TOKEN_SECRET` and `MYTMS_AGENCY_CHOICE_TOKEN_SECRET`. Their non-secret `*_KEY_VERSION` and `*_READ_KEY_VERSIONS` values belong in `candidate-broker/wrangler.jsonc`; the secret values never belong in source. Before accepting an invitation deployment, list secret **names only**, prove both authorities exist, and run the real TEST invitation inspection plus invitation-bound challenge-start probe. A successful invitation inspection does not prove password setup: a missing global-challenge secret fails the next call as `CANDIDATE_BROKER_KEY_VERSION_UNAVAILABLE`. Never print, reuse or alias either secret, and never treat a Git deployment as proof that dashboard-managed secrets exist.

Federated Candidate login additionally requires the same agency-specific `CANDIDATE_FEDERATED_IDENTITY_SECRET` secret binding on the normal backend and the corresponding real private Candidate Worker. When `CANDIDATE_FEDERATED_ROUTING_ENABLED=TRUE`, private readiness must fail if this binding is absent. Before accepting a Candidate login deployment, list secret names only on both Workers, prove the readiness route, select a real TEST agency through the broker, and prove private bootstrap succeeds. A 200 health check that does not exercise agency projection is insufficient. Never print, reuse across agencies/environments, or commit the secret value.

### Safe UPGRADE and NEW-database requests

Treat `CLOUDTMS PLAN UPGRADE <exact target>` and `CLOUDTMS PLAN NEW DATABASE <agency>` as the canonical requests for the Miget installation/release system. Both are read-only planning requests: inspect the exact target, report the precise pending migrations and new/changed repeatables plus required PostgREST, gateway, Worker and configuration work, and stop before mutation. A vague request containing “upgrade”, “new database”, “prepare”, or “what is missing” never authorises APPLY, provisioning, deployment, paid-resource creation, secret changes, feature activation or resource reallocation.

Current LIVE's first deliberate schema promotion uses `LEGACY_UPGRADE`; moving its hosting to Miget did not perform that upgrade. A managed database uses `UPGRADE`, and a proved-blank database uses `NEW`. The owner has granted standing authority for managed Miget TEST `UPGRADE` releases: do not stop to request a new APPLY phrase. Dispatch the protected workflow against the exact current `test` branch head; it must source-gate that commit, run its read-only plan, generate the commit-bound engine approval internally, apply only pending migrations and new/changed repeatable closures, and complete every security/contract verifier. This standing authority never covers LIVE, NEW, ADOPT, LEGACY_UPGRADE, destructive SQL, payment/provider actions, secret changes, or unrelated application-data mutations; those retain their separate exact authority. Never extend an APPLY to another database or operational stage by inference.

## Secrets and sensitive data

### Daily Candidate first-submission regression boundary — AV-962

Daily entry starts from the exact booked Rota source/date, not a Contract Week or an existing Timesheet. Preserve the 63 Candidate operations and the closed BOOKED_DAILY_SHIFT target. Reading a shift or saving a local draft must not create a database Timesheet; first-row admission requires explicit submission, source/current-generation checks and a locked, replay-safe booking family. Different worked dates in one week remain different Daily Timesheets; withdrawal/resubmission retains one current version per booking and revoked history.

Ordinary MyTMS first-entry eligibility is a started booked shift in the current complete published Rota window, not the legacy four-hour flag. Preserve the legacy/emergency windows unchanged. Once a real Daily Timesheet exists, its current booking family remains viewable/cancellable/resubmittable as permitted after Rota rollover; never require the vanished source for that existing-record lifecycle. The mandatory booked-source verifier must remove the active generation before cancellation and prove resubmission/PHONE receipt without a duplicate current record. A never-submitted local draft does not extend source authority or create a Timesheet.

The Candidate receipt adapters may record factual hours and verified PHONE signatures/evidence before Office Client/role/rate resolution. They must not invent a Client, rate, TSFIN row or financial hash, stamp Office authorisation, invoke the signed legacy submit route, or change canonical calculation/processing/authorisation owners. With a real financial row, reset delegates to the existing owner; the narrowly proved no-finance receipt branch rotates only the Timesheet family. Keep normal triggers and derived rechecks, protected-history locks and browser-denied private helpers. Later Office resolution must preserve both signatures and the official signed document on the same current Timesheet. Candidate/manager copy is normal “Awaiting Office authorisation”, never internal resolution warnings.

Keep `28082026_1858_candidate_daily_booked_source_verification.sql` mandatory in both NEW and UPGRADE. Its rollback-contained first use covers first admission, PHONE approval/receipt, separate dates, mixed Weekly/Daily pagination, withdrawal/resubmission, Office rejection after Rota removal, later Office resolution with document preservation and protected/stale/foreign negatives. Generated catalogue proof must show only the intended Candidate routine delta; no financial, trigger, table or policy changes. Source tests and mock render receipts do not replace deployed API, real-document and physical-phone acceptance.

Daily response tests must include the actual PostgreSQL null-stripped shape of future booked days. Only `break_entry.applicable=false` with `source=NOT_APPLICABLE` may normalize an absent `mode` to explicit null; editable break authority, context proofs and undeclared-field rejection remain strict. Test both private and public response boundaries: a valid future day must not turn an otherwise complete Rota into a false readiness failure.

Daily submission adapter tests must use the actual app's local `actual_schedule_json` (`start_time`/`end_time`) after adaptive-break normalization, not only hand-built `worked_*_iso` fixtures. The Candidate adapter must reuse the unchanged `mapCanonicalDailyScheduleToIso` and `ukLocalToUtcISO` authorities for that factual conversion, retain exact workflow work-date/one-interval checks and prove overnight/DST/break cases. Do not alter financial calculation/Office owners or require the app to duplicate UK timezone authority to repair a payload mismatch. Source or SQL fixture success is not physical PHONE submission proof.

Daily official document proof must include a worked date different from the week ending and an unresolved Client. Use the saved Timesheet week-ending date before any worked-date fallback, and freeze the booked `hospital_norm` from the Daily receipt even when no Client/TSFIN mapping exists. The review and final signed copies must show the same factual hospital, ward, role, hours and period without inventing a financial Client. Keep the actual-read-projection and full render-model cases in `tests/candidate-daily-official-document.test.js`; preserve immutable prepared documents and obtain a new submission generation rather than overwriting an earlier review pack.

Daily recovery/list regressions (28 August 2026): a non-editable historical Daily record must not resolve break-entry settings that it cannot use; retain exact Client-setting validation for editable records. Keep the rollback-contained `28082026_2203_candidate_daily_inapplicable_break_verification.sql` mandatory in NEW and UPGRADE. Cancelling an interrupted Daily PHONE review must preserve the permitted Daily PHONE route, not reset to the Weekly ELECTRONIC route. The booked-source first-use verifier must cancel/replay/reselect the manager request, preserve the review pack and then complete manager approval and receipt. Do not weaken the existing workflow route constraint or alter finance to repair either defect.

Daily receipt display regressions: an unresolved server-owned Candidate receipt may already have a genuine UNASSIGNED financial snapshot whose hours are zero. Its app display must retain submitted factual minutes while unresolved; resolved, protected and import-owned rows keep the existing financial precedence. Daily list manager status must reuse the exact request/generation decision already supplied by detail, not infer approval from receipt state. Include the pending-zero-snapshot case and before/after financial-row equality in the mandatory booked-source verifier. Required JSON outputs must use null-safe assertions (`IS DISTINCT FROM`); `value <> expected` inside PL/pgSQL `IF` silently accepts a missing/null value and is not an adequate required-field check.

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

`COALESCE`, `NULLIF`, `LEAST`, and `GREATEST` are PostgreSQL syntax constructs, not schema-qualified `pg_catalog` functions. Never write `pg_catalog.coalesce(...)`, `pg_catalog.nullif(...)`, `pg_catalog.least(...)`, or `pg_catalog.greatest(...)`. PostgreSQL can accept the routine definition and then fail with SQLSTATE `42883` only when that statement first executes. Keep a source guard for all four prefixes and run a rollback-contained real first-use call against the complete pending installed-state sequence before publication and after protected APPLY; routine compilation and ledger proof alone are insufficient.

The Candidate/MyTMS boundary in `supabase/release/protected-boundary-lock.json` is frozen. If proposed work needs to alter one of those files or named objects, stop and coordinate before editing. All Banking Pay changes must also preserve Policy X.

Rota first-use regression (28 August 2026): extract JSON text before concatenating it. Write `prefix || (item->>'key')`, never `prefix || item->>'key'`. The latter can compile inside PL/pgSQL but fail at runtime with `42883` or `22P02`. In Rota publication this rolled back a complete batch when booked/blocked days cleared existing availability; PostgREST surfaced `42883` as HTTP 404 even though the RPC existed. Do not misdiagnose that HTTP status alone as a missing RPC or change credentials. Keep `28082026_1236_candidate_daily_rota_clear_key_verification.sql` mandatory: it exercises blank publication, a mixed linked/unlinked batch, booked and blocked clearing, unchanged dates, exact keys, idempotent replay and service-only ACL, with all fixture rows rolled back.

Rota response-contract regression (28 August 2026): a successful database tiles call and visible Home Rota card do not prove the Rota page works. The existing DAILY tiles response includes optional/null `shift_starts_at` and `shift_ends_at` values; both private and public Worker validators must accept valid timezone-qualified timestamps without admitting any undeclared field. Keep the two-boundary tests in `tests/candidate-daily-phase1b-contract.test.js`, including booked overnight shifts, blank days and invalid timestamps. Prove the original live response passes the validator, deploy compatible private/synthetic Workers before the public broker, then open Rota on the device. Do not remove the time fields, loosen the whole response contract or change database entitlements to conceal a Worker projection mismatch.

Rota operational freshness regression (28 August 2026): the strict 120-second authority-cutover proof is not a lifetime for unchanged day/event-published Rota data. The last complete ACTIVE generation remains readable across UK midnight until replaced atomically; retain its actual 14 dates and real publication time, never relabel old dates or invent an unpublished day. `_candidate_daily_freshness_v1` preserves incomplete/future-generation, projection-lag, terminal-outbox and identity checks. The existing tiles RPC accepts today's Candidate request while returning that retained window; the existing availability command must reject past and unpublished dates. Keep `28082026_1321_candidate_daily_calendar_freshness_verification.sql` mandatory in UPGRADE and NEW: real Candidate first-use reads/writes, capability, 23/25-hour DST, midnight retention and incomplete rows must pass. Do not fake freshness by changing timestamps or bypassing failed projections. Separately prove the Google continuation publishes the next complete window; the controlled authority-transition freshness and reconciliation requirements remain unchanged.

Google availability projection lease regression (29 August 2026): the server-owned lease must exceed the complete bounded Google read/write/flush/ack window; the caller must never choose it. The Google drain must bulk-read shared Sheet state once per claim, reserve time for the completion acknowledgement, stop starting new cell work at its cooperative budget, flush before acknowledgement, and validate every returned completion outcome rather than treating HTTP 200 alone as delivery. A genuinely new claim may recover only an expired `CLAIMED` outbox lease: lock a bounded `p_max_items` set with `FOR UPDATE SKIP LOCKED`, increment the attempt, apply retry/terminal backoff, and clear its old token before any later claim. Never alter an active lease; never let a late old token complete a re-leased item; and preserve the completed-claim replay rule requiring status after expiry. Keep the static verifier mandatory in UPGRADE and NEW and retain rollback-contained PostgreSQL 17 first-use tests for expired, active, stale-token, fresh-token, retry and terminal cases.

Google Rota reconciliation scalability regression (29 August 2026): a full Availability refresh must inspect the complete Candidate list, not stop at a convenient first page, but it must not post fourteen dates for every non-enrolled Candidate. Probe each Candidate identity once through the signed reconciliation route, treat only the server-owned `NOT_ENROLLED` classification as a benign skip, and then reconcile the full current date window only for identities the server proves are linked. Ambiguous, invalid and unknown results remain terminal failures. In the database, apply every linked date first and refresh the Candidate sync state exactly once per linked Candidate, never once per date. Keep `29082026_0923_candidate_daily_reconciliation_linked_window_verification.sql` mandatory in UPGRADE and NEW and retain interruption/resume coverage proving that all Candidate rows are probed, acknowledged cursors are not replayed and no contact/HMAC identity is persisted in Apps Script continuation state.

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
* Keep the data-free contract provider- and upgrade-history-neutral without weakening business/security coverage. Compare columns by name rather than physical `attnum`; exclude only routine-local `plpgsql_check.*` diagnostic instrumentation and the Supabase-only `authenticator` grant on `cloudtms_data_api_mfa_gate()`; retain every functional routine config, normalized definition hash, browser/service ACL, RLS flag and policy. If managed TEST has an intentional policy that a clean NEW replay lacks, add an exact additive source migration that verifies/recreates it. Never replace the repository contract with a hosted export merely to make an unexplained drift pass.

For every new Candidate-named `SECURITY DEFINER` function, update and locally test the active exact Candidate security inventory named in `supabase/release/current-release.json` in the same change (currently `27082026_1947_candidate_named_security_verification_v3.sql`; retain historical v2 unchanged). The expected function count, service-missing count, browser-executable count, and complete catalog fingerprint must all be reconciled from a real PostgreSQL 17 verification result; never leave the verifier pinned to the previous function set. Also compile every changed repeatable in disposable PostgreSQL 17 before protected APPLY so PostgreSQL-only syntax errors are caught before the release reaches Miget.

Rota-only removal has an explicit boundary: clear only the exact Candidate Global CID/Rota access and retain account, membership, Contract/Timesheet/evidence and source/generation history. Clearing `key_norm` legitimately invokes the existing invoice, Timesheet-finance and Banking derived rechecks; never suppress these triggers or claim zero downstream work. Require the registered MASTER purpose/request/target-bound authority, exact idempotency receipt, current-booking and pending/uncertain-effect guards, and a fresh post-removal Create operation for same-identity re-enrolment. Keep `28082026_1657_candidate_google_rota_removal_verification.sql` mandatory in NEW and UPGRADE; it proves preserved business rows, ordinary derived rechecks, replay, stale publication rejection and mixed-Candidate progress. Do not call or replace generic Candidate deletion or add a 64th Candidate operation.

A clean `NEW` release must apply every locked migration after the release-control anchor in chronological order after the immutable baseline/bootstrap; it must not treat the baseline date as permission to omit later migrations. `NEW` may use a separately declared portable verifier set only to exclude fixtures that require historical TEST business rows. It must still run every browser-isolation, RLS/ACL, Candidate/MyTMS, private-helper and rollback-contained first-use verifier and must generate the same current repository contract as `UPGRADE`. After all functional repeatables, install the current final browser-isolation repeatable that revokes `PUBLIC`, `anon` and `authenticated` execution from public `SECURITY DEFINER` functions except the established Data API MFA pre-request gate. No later repeatable may reintroduce public browser execution without updating that final closure and every exact security fingerprint from a real PostgreSQL 17 replay.

Banking Pay replacement-session candidate work is session-bound authority, not a copyable queue payload. Never replay a Candidate source-build job into a replacement session by changing only its session fields while retaining the old source sequence/build identity. Existing target scope must be re-established through `pay_workbench_enqueue_candidate_refresh`; not-yet-seeded scope must remain with the active target root; persisted legacy raw replays must be terminalised and rebound under the Candidate serial lock before any source-build claim. Keep `29082026_0614_banking_pay_replaced_candidate_owner_verification.sql` mandatory in NEW and UPGRADE. It must prove canonical current ownership, valid terminal job shape, deferral, idempotency, service-only ACL and no Draft/provider/payment/settlement action.

A discarded Banking Pay session must not retain queued or running Candidate source-build work that globally blocks a current open session for the same Candidate. The service-only repair may terminalise only `WORKBENCH_CANDIDATE_SOURCE_BUILD` jobs whose owning session is proved `DISCARDED`, and must take the Candidate serial lock plus registry/build/job/attempt locks, obsolete any active attempt and unfinished build, preserve every open-session owner, revalidate the current pending owner, recompute progress and remain idempotent. Keep `29082026_0720_banking_pay_discarded_session_blocker_verification.sql` mandatory in NEW and UPGRADE; it reproduces the real cross-session blocker and proves the current/open-owner, service-only, first-use, terminal-shape, idempotency and Policy X boundaries.

A version-one Banking Pay Workbench session has no previous session version and must never call the same-session rebase helper with version zero. `pay_workbench_session_refresh_current_authority_v1` must skip only that impossible rebase at version one and continue through the existing canonical Candidate enqueue path; sessions above version one retain the exact existing rebase attempt. Keep `29082026_0804_banking_pay_version_one_refresh_verification.sql` mandatory in NEW and UPGRADE, with rollback-contained first use, replay/no-duplicate proof, service-only ACL and no Draft creation.

Security-inventory fingerprints must be provider-neutral. Never hash `oid::regprocedure::text` or another signature display that changes with the session `search_path`. Build the signature from explicit catalogue identities: function schema/name plus each input type's schema/name from `pg_proc.proargtypes`, `pg_type` and `pg_namespace` in argument order. Prove the count, privilege counts and hash in the clean PostgreSQL 17 rebuild and the protected Miget target; a matching privilege count with a display-only hash mismatch is a verifier defect to correct, never a reason to weaken or skip the verifier.

Installed-state security verifiers must distinguish one-time bootstrap evidence from permanent operational invariants. Once TEST activation is explicitly authorised, a permanent release verifier must not require empty business tables or require every Candidate feature flag to be false. It must continue to prove the TEST environment, valid typed configuration, RLS, grants, browser isolation, function security and every other durable safety boundary. Put bootstrap-only empty/disabled assertions in a separately invoked bootstrap proof, not in every later UPGRADE.

Any migration or repeatable that changes a contracted relation, routine, trigger, policy, grant or default ACL must have an exact generated contract diff reviewed and committed before protected APPLY. `npm run db:check` alone is not contract-drift proof. Generate from a verified disposable PostgreSQL 17 rebuild before publication; if an already-started TEST release exposes previously unrecorded installed drift, use the protected read-only TEST contract-export workflow, review every changed object, and stop on anything outside the intended source. Contract export must deduplicate ACL entries after mapping Miget's physical owner and the logical repository owner to `postgres`, so provider-normalisation collisions cannot create false drift.

A green protected PLAN is not proof that `supabase/release/current-contract.json` describes a changed migration or repeatable: PLAN deliberately does not install pending definitions. Before the first APPLY, prove the generated contract from a disposable PostgreSQL 17 rebuild of the exact commit and include the reviewed contract file in that same commit. Never use APPLY as the mechanism for discovering a stale contract. If an APPLY has already installed the intended definition but stopped before recording `VERIFIED` solely because the approved contract was stale, do not edit ledgers or reapply source blindly: run the protected read-only contract export at that exact installed source, prove the object-by-object diff is limited to the intended definitions, commit that data-free contract, rerun PLAN, and then rerun APPLY.

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
