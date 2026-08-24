# CloudTMS Miget Operations gateway

Repository authority for the permanent TEST-only Cloudflare Worker that provides:

- the /rest/v1 compatibility proxy used by CloudTMS TEST Workers;
- the read-only remote MCP used by ChatGPT web;
- direct read-only PostgreSQL catalogue and RPC-definition inspection for the agency TEST and MyTMS databases;
- separate agency and MyTMS migration/repeatable-ledger inspection;
- fixed-query RLS/grant/SECURITY DEFINER security review and aggregate performance/lock diagnostics;
- read-only Miget infrastructure discovery.

The Worker is codex-cloudtms-miget-gateway. Its stored Cloudflare secrets and Hyperdrive credentials are not regenerated or rotated by an ordinary deployment. The ChatGPT connector capability and Miget API token have no application-enforced expiry; access continues until an administrator disconnects the connector or deliberately revokes or rotates a credential.

Never put token values or PostgreSQL URLs in this directory. Deployments require the already-provisioned bindings and these secret names:

- MIGET_API_TOKEN
- MIGET_MCP_ROUTE_TOKEN
- MIGET_POSTGRES_SERVICE_ID
- MYTMS_MIGET_POSTGRES_SERVICE_ID

Run npm ci, npm run check, then npm run deploy:dry. A real deployment is TEST infrastructure mutation and still requires explicit authority. After deployment, prove /miget-gateway/health, an agency TEST PostgREST read, both MCP database targets, and the ChatGPT-web RPC-definition retrieval.

The MCP database target names are `agency_test` and `mytms_test`. Available tools are `miget_verify_codex_parity_route`, `miget_list_infrastructure`, `miget_inspect_postgres`, `miget_db_catalog_summary`, `miget_db_release_ledger`, `miget_db_security_audit`, `miget_db_performance_summary`, `miget_db_list_rpcs`, and `miget_db_get_rpc_definition`. The ledger tool uses `private.cloudtms_migration_ledger`/`private.cloudtms_repeatable_ledger` for `agency_test` and `public.schema_migrations`/`public.schema_repeatables` for `mytms_test`.

Current bindings are Hyperdrive `11c78f14afea494c9d5e8d8ad57d41a2` for agency database `cloudtms_test_clone` and `7e979a8127c84c319dfc2ecf488aa903` for MyTMS database `uofvkfi5`. Hyperdrive query caching is disabled so audits do not return stale catalog/ledger evidence. Both underlying credentials remain private; the Worker exposes only fixed read-only transactions with statement/lock/idle timeouts.
