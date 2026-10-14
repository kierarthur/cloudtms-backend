# Kier1978 agency TEST rollback connector

Account: `kierarthur1978@gmail.com`. Owner explicitly requested installation on 14 October 2026. This dedicated Worker binds only agency TEST Hyperdrive; its runner requires physical database `cloudtms_test_clone`. No MyTMS or LIVE binding, no durable release tool.

Worker: `codex-kier1978-miget-transaction-lab`; MCP/plugin: `cloudtms-miget-transaction-lab-kier1978`; endpoint: `https://codex-kier1978-miget-transaction-lab.kier-88a.workers.dev/mcp`.

Its own generated credential is kept only as Worker secret `MIGET_TRANSACTION_LAB_ROUTE_TOKEN` and Windows user environment variable `CLOUDTMS_MIGET_TRANSACTION_LAB_KIER1978_TOKEN`. No existing account credential, connector or Worker is modified. Operations remains read-only.

The eight source/build/test inputs in `INHERITED_SOURCE_PINS.json` are exact copies of the existing transaction lab; no SQL engine change is introduced. Independent source packaging makes this new account deployment reproducible without committing unrelated pre-existing changes. The inherited internal health service name is generic; the configured Worker and endpoint identify this account-specific deployment.

Envelope: 2,000,000 SQL bytes and 250 statements; outer transaction escape is refused; the server owns unconditional rollback. Optional verification uses a fresh read-only connection. Require `accepted=true`, `database=cloudtms_test_clone`, `rolledBack=true`, `rollbackVerification.connectedFresh=true` and passing absence assertions. Sequence increments may leave gaps; never invoke external payment/provider/email/network effects. HealthRoster trials additionally require its shared turn-taking lock.

Full Codex restart and a new task are required for newly registered native MCP tools. Hosted authenticated HTTP proof and CLI registration are separate from native tool pickup. See `ACCEPTANCE.json` once setup proof is complete. HealthRoster implementation remains paused.
