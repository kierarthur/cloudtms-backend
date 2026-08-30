# CloudTMS Miget Operations gateway

Repository authority for the permanent TEST-only Cloudflare Worker that provides:

- the /rest/v1 compatibility proxy used by CloudTMS TEST Workers;
- the read-only remote MCP used by ChatGPT web;
- direct read-only PostgreSQL catalogue and RPC-definition inspection for the agency TEST and MyTMS databases;
- separate agency and MyTMS migration/repeatable-ledger inspection;
- fixed-query RLS/grant/SECURITY DEFINER security review and aggregate performance/lock diagnostics;
- read-only Miget infrastructure discovery.
- an authenticated, TEST-only database-path benchmark harness under `/__debug/db-benchmark`.

The Worker is codex-cloudtms-miget-gateway. Its stored Cloudflare secrets and Hyperdrive credentials are not regenerated or rotated by an ordinary deployment. The ChatGPT connector capability and Miget API token have no application-enforced expiry; access continues until an administrator disconnects the connector or deliberately revokes or rotates a credential.

Never put token values or PostgreSQL URLs in this directory. Deployments require the already-provisioned bindings and these secret names:

- MIGET_API_TOKEN
- MIGET_MCP_ROUTE_TOKEN
- MIGET_POSTGRES_SERVICE_ID
- MYTMS_MIGET_POSTGRES_SERVICE_ID

Run npm ci, npm run check, then npm run deploy:dry. A real deployment is TEST infrastructure mutation and still requires explicit authority. After deployment, prove /miget-gateway/health, an agency TEST PostgREST read, both MCP database targets, and the ChatGPT-web RPC-definition retrieval.

## TEST database-path benchmark

The benchmark harness compares the existing HTTP/PostgREST path with direct PostgreSQL through only the agency TEST `HYPERDRIVE` binding. It does not accept a database target, does not reference the MyTMS or LIVE bindings, supports only fixed read-only queries, and leaves the normal `/rest/v1` proxy unchanged. Hyperdrive query caching must remain disabled. Worker placement is pinned to the agency TEST PostgREST hostname so a deployment does not discard the existing targeted-placement setting.

All benchmark routes require `Authorization: Bearer <MIGET_MCP_ROUTE_TOKEN>`. PostgREST and comparison routes also require the existing TEST service-role JWT in `x-cloudtms-benchmark-postgrest-token`; neither credential belongs in arguments, source, logs, or result files. The available routes are:

- `/__debug/db-benchmark/postgrest`
- `/__debug/db-benchmark/hyperdrive`
- `/__debug/db-benchmark/compare`
- `/__debug/db-benchmark/metadata`

Tests are `select_1`, `small_read`, and `multi_read`. The `multi_read` test requires a TEST timesheet UUID. Hyperdrive supports `variant=sequential` for like-for-like round trips and `variant=optimized` for a single-query version. PostgREST supports `variant=optimized` for the same consolidated SQL through one diagnostic RPC, which isolates the benefit of fewer round trips from the transport choice. The runner is deliberately serial and writes only timings, counts, structural hashes, and aggregate metadata:

```powershell
$env:MIGET_MCP_ROUTE_TOKEN = '<load from approved ignored storage without printing>'
$env:MIGET_SERVICE_ROLE_JWT = '<load from approved ignored storage without printing>'
npm run benchmark:db -- --base-url https://codex-cloudtms-miget-gateway.kier-88a.workers.dev --samples 100 --warmup 10 --cold 5
```

The fixed MCP database target names are `agency_test`, `mytms_test`, and `agency_live`. Available tools are `miget_verify_codex_parity_route`, `miget_list_infrastructure`, `miget_inspect_postgres`, `miget_db_catalog_summary`, `miget_db_release_ledger`, `miget_db_security_audit`, `miget_db_performance_summary`, `miget_db_list_rpcs`, and `miget_db_get_rpc_definition`. The ledger tool uses `private.cloudtms_migration_ledger`/`private.cloudtms_repeatable_ledger` for both agency targets and `public.schema_migrations`/`public.schema_repeatables` for `mytms_test`. All database tools execute fixed read-only catalog queries; the connector does not accept arbitrary SQL.

Current bindings are Hyperdrive `11c78f14afea494c9d5e8d8ad57d41a2` for agency database `cloudtms_test_clone` and `7e979a8127c84c319dfc2ecf488aa903` for MyTMS database `uofvkfi5`. Hyperdrive query caching is disabled so audits do not return stale catalog/ledger evidence. Both underlying credentials remain private; the Worker exposes only fixed read-only transactions with statement/lock/idle timeouts.
