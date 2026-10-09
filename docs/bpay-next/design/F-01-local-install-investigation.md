# F-01 / F-23 local PostgreSQL installation investigation

Read-only source investigation, 9 October 2026. Reviewed BUILD-BE base: `a6f7423eb71e6e7cd173e705c2290e5408a1d077`. No SQL, Docker, runtime, fixture, hosted operation or credential read was performed. Policy X: no drift.

## Binding build scope

The owner start sheet `BUILDER_START_SHEET_BPAY_NEXT_R3.md` §§3–4 overrides plan B2.23/F-23's broader installation-proof language: during the build load SQL, run `db:check`, changed-JavaScript `node --check`, local contract seal/coupling and normal source gates. Do not run fixtures, rollback rehearsals, scenario tests or performance checks; the sole exception is the prescribed phase-3 300-Timesheet Create Draft timing check. Retain the repository's complete NEW/UPGRADE verifier manifests for the later authorised release.

## Container and provider prerequisites

Use the single task container `bpay-next-build-pg17`, cached `postgres:17.11-bookworm`, exact database `banking_modal_v2_test`, proposed loopback binding `127.0.0.1:55439:5432`. Root must first inspect disk/Docker inventory, confirm the name and port are free and reuse the cached compatible image. Task resources remain until PF-16/R2 and must then be removed with their task volume/image/cache ownership reconciled.

Every local script must reject anything except URL hostname `localhost`/`127.0.0.1`, exact database name, PostgreSQL major 17 and `CLOUDTMS_ALLOW_LOCAL=1`; require `CLOUDTMS_LOGICAL_POSTGRES_OWNER=CURRENT_USER`. Do not print a URL, password or environment dump. Use task-scoped generated local authentication only and record name/image/port/owner role, never credentials, in BUILD_LOG.

Connect the schema loader directly as a dedicated local release LOGIN role with `NOSUPERUSER NOBYPASSRLS`; `current_user` must equal `session_user`. Do not connect as the stock superuser `postgres`, or emulate the owner using `SET ROLE`. `scripts/cloudtms-managed-release.mjs`'s exact `OWNER_RLS_ENVELOPE` applies to `supabase/migrations/23082026_1337_manager_email_candidate_identity_defaults.sql` and requires this role posture, `row_security=on`, and the original forced-RLS table posture. Reuse its bounded temporary-owner-policy envelope; never edit the immutable migration or persist the temporary policy.

The reusable prerequisite source is `tests/fixtures/28082026_1229_banking_modal_local_pg17_prerequisites.sql`. It supplies empty `auth.users`, `auth.uid/role/jwt`, and local `vault.secrets/decrypted_secrets/create_secret` provider APIs; these are provider installation surfaces, not business fixtures. Its stock profile creates `service_role BYPASSRLS`, `authenticator` and `supabase_admin`, so it must not be claimed to be exact Miget parity unchanged. A task-local executable prerequisite profile must keep `service_role NOBYPASSRLS`; omit the Supabase pool role `authenticator` when matching Miget, while using the current owner adapter that already omits its revoked grantee and MFA grant. Preserve any `supabase_admin` API/ACL surface required by baseline; do not broaden application permissions.

Required baseline extensions are `pgcrypto` and `uuid-ossp` in `extensions`, and `pg_trgm`/`btree_gist` in `public`. Migration `24082026_1406_enable_pg_stat_statements.sql` adds `pg_stat_statements` in `extensions`; preinstall that non-trusted extension as the container administrator before switching to the non-superuser loader. Configure the stock container with `shared_preload_libraries=pg_stat_statements`, `jit=off`, and a connection/session `statement_timeout=120s` for parity. Stock PostgreSQL has no Miget `pg_show_plans` library; do not invent or install it just for this schema copy. Provider schemas/functions and app database ownership/grants must allow the release owner to execute the unchanged baseline and bootstrap.

The immutable bootstrap generates its own fresh local invoice HMAC through the local Vault API, inserts safe system configuration and keeps Candidate/MyTMS/workbench automation disabled. That system-only initialization is required for NEW; never read its generated secret and never substitute business/customer rows, enable NEXT, insert a TEST-agency simulation marker or activate providers.

## Exact schema installation order and adapters

Use `supabase/release/current-release.json` as the declarative path authority. Its 25 `baselineFiles`, in array order, are structural baseline; routines pages `00` through `20`; views/triggers; structural finishing; ACL baseline. Then use its exact `controlPlaneMigration` (`22082026_1507_cloudtms_database_release_control_plane.sql`) and exact `bootstrapFile` (`22082026_1506_cloudtms_new_database_bootstrap.sql`). Do not install every historical migration over the structural baseline.

Use the current engine's `assertBootstrapPrefix`/`compileBootstrap`, which validate the original migration prefix and bootstrap pin, combine baseline/control-plane/bootstrap in one guarded transaction, and receipt only the original prefix and baseline-repeatable inventory. Apply all migrations after the control-plane anchor in `inventory()` order. Apply only current repeatables whose closure hash differs from `baseline-repeatable-lock.json`, again in `inventory()` order. `inventory()` includes new root SQL on disk as well as tracked roots and orders by `sqlDateKey()`; ordinary filename sorting is unsafe.

Use `mapExecutableSqlSource`, `readManagedClosure`, `compileManagedUnit`, `executableSqlFile(file,{canonical:true})` and `unitCheckpointSql` rather than a handwritten owner-string replacement or transaction stripper. These preserve LF canonical bodies, recursive includes, the hash-bound dynamic CURRENT_USER mapping for `04102026_1827` destination-group ownership, pinned non-atomic exceptions and the exact RLS envelope. Acquire `acquireWriterSql` on one persistent `openManagedPsqlSession`, preserve/reassert that writer guard and reset the session using `resetWriterSql` between units. A source hash/cache mismatch must fail before execution.

Preserve the engine's Weekly Source reader split: `readerReleasePhases` installs ordinary units first; `prepareManagedSourceReaders` prepares the existing empty database inventory; `readerActivationSql` activates exactly the two readers together. Those two are `04082026_1146_pay_workbench_timesheet_input_fingerprint_v1.sql` and `04082026_2314_pay_workbench_unit_economic_occurrence_page_v1.sql`. This is installation, not a synthetic data rehearsal.

The full `node scripts/cloudtms-db-release.mjs apply --environment=TEST --mode=NEW --expected-database=banking_modal_v2_test` is **not** the build-time command: its `applyManagedRelease` path unconditionally calls `runBankingPayCatalogPreapply` (a rollback catalogue rehearsal), then every `newVerificationFiles` entry, many of which create rollback business fixtures. There is no supported CLI flag to skip those calls. Do not weaken the engine or modify release inventories to manufacture a build result. The F-23 local-only loader must use the existing source/install adapters but stop at schema compilation and contract sealing. If it uses adapter-produced bootstrap/unit receipts, do not manually repair them or record a protected release `VERIFIED`; record an external local source checkpoint and label the result `LOCAL_SCHEMA_COMPILED`, with full protected verification still pending.

## Working-tree apply and sealing

F-23 needs a named local-only wrapper because `seal-local --replay-from=<commit>` reads only `git diff <base>..HEAD`; it cannot see uncommitted or untracked changes. The historical `.codex-tmp/compile-disposable-agency-release.mjs` is not reusable: it assumes one exact 2026-August migration/65 repeatables, reads a second database's ledger, lacks strict local target guards and slices an old verifier array.

The new wrapper should snapshot canonical `inventory()` path/hash/recursive closure evidence in a task-local checkpoint. Before loading a batch, re-read source, verify prior migration hashes, refuse deleted authority or modified previously installed migrations, discover changed/new migration roots and changed repeatable closures against that checkpoint, then compile/apply the entire selected envelope in canonical order. Include the F-04 successor-map final definers for touched objects and the final ACL/browser closures after touched historical definitions. The current final closures are `24082026_0322_miget_postrestore_rpc_browser_isolation.sql`, `26082026_0230_general_security_definer_browser_isolation_v2.sql`, and `15092026_1534_weekly_source_acl_contract_v1.sql`. Preserve unchanged historical `22082026_1402_candidate_named_security_definer_browser_isolation.sql` as its baseline receipt; do not force its old exact inventory against newer routines. Root corrected this investigated recommendation after the initial real schema-load failure. Do not assume an unchanged independent closure will be selected automatically by changed-only apply. Register exact selected paths/hashes in the checkpoint; do not invent `db:apply` CLI flags.

Once working-tree SQL has been loaded, use this supported sequence:

```text
npm run db:check
<the named F-23 local-only working-tree apply procedure>
npm run db:contract:seal-local
<review contract diff; stage explicit SQL, appended lock, metadata and contract paths>
<one local commit containing SQL and generated contract together>
npm run db:contract:coupling
```

`seal-local` enforces PG17, local target and ownership of `private`; it exports the contract but does not apply source without replay mode. The wrapper must load LF canonical executable files even on Windows. `coupling` checks HEAD and tracked working-tree changes, but ordinary `git diff` does not enumerate untracked SQL, so the loader/checkpoint must cover it independently. If contract bytes are identical, update the existing reviewed `supabase/release/contract-neutral-changes.json` attestation in the same commit. Do not commit a SQL-only checkpoint followed by a second contract-only commit.

## Verification scope

During this build, the owner's start-sheet checks govern. Do not execute the complete `verificationFiles`/`newVerificationFiles` arrays, protected API probes, rollback first-use fixtures or an ad-hoc caller harness. Catalogue-only ACL assertions in general browser isolation, Candidate-named isolation, Candidate/MyTMS isolation, Miget private-helper ACL and Weekly Source ACL verifiers are identifiable from existing source and are suitable candidates for F-23's normal source/catalogue installation checks if incorporated within the approved loader scope; they must never be substituted for complete release verification or use synthetic rows. Preserve every actual release verifier in the manifest for the later authorised TEST installation. F-24 must state that schema compilation and a sealed catalogue contract do not establish app acceptance or protected release verification.
