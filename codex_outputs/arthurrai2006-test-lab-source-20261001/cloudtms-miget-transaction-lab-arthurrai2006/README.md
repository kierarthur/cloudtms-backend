# Arthur's independent TEST transaction lab

Account: arthurrai2006@gmail.com. User approved a separate connection and new credential; no existing tokens may be rotated.

Reuses adjacent `cloudtms-miget-transaction-lab/src/index.ts` and its unchanged runner/envelope. No shared source or existing Worker was modified. Only agency TEST Hyperdrive is bound. No MyTMS or LIVE access, no durable deployment tool. Existing Operations connector remains unchanged.

Worker: `codex-arthurrai2006-miget-transaction-lab`.
MCP/plugin: `cloudtms-miget-transaction-lab-arthurrai2006`.
User environment variable: `CLOUDTMS_MIGET_TRANSACTION_LAB_ARTHURRAI2006_TOKEN`.
Worker secret: `MIGET_TRANSACTION_LAB_ROUTE_TOKEN` (new Worker only).

Local envelope tests: 15 passed, 2 local integration tests skipped without a configured disposable database URL. Dry bundle and plugin validation passed. Hosted proof: anonymous 401; authenticated tools/list 200; CREATE/INSERT/SELECT of an isolated probe table returned one temporary row, then rolledBack=true, connectedFresh=true and temporary_table_removed=true on cloudtms_test_clone. No business rows changed. Proof SQL SHA256: cd4e9e959bf1b706ea64909613d8ffc3fd8edb0459b6feaf15cfe962263539ae.

Installed and enabled via plugin add and explicit codex mcp add. Full Codex restart/new task is required for native tool pickup; that has not yet been proved. The shared source health name still names the original lab because its implementation was deliberately unchanged. Worker endpoint/config identify this separate deployment.

Never run external-effect functions. Sequence increments are not transactional. All durable releases still require the protected commit-bound workflow. This setup does not repair or complete the pending Weekly Source release.
