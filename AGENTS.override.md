# CloudTMS Backend — Desktop Override

This is a Codex desktop workspace, not the former `/workspace` cloud container.

The authoritative shared rules are in the parent workspace file:

```text
C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\AGENTS.md
```

Apply that file in full. Ignore cloud-only assumptions in the legacy `AGENTS.md` in this directory, including `/workspace` paths, frontend-primary container ownership, backend GitHub read-only status, forced backend restoration, and mandatory patch/replacement handoff solely because of cloud permissions.

Backend-specific desktop rules:

- Work directly in this local backend clone when backend changes are requested.
- Preserve unrelated local and user changes, including dirty SQL files.
- Do not restore, commit, push, or deploy changes unless explicitly requested.
- Do not modify `wrangler.toml` unless the user explicitly requests that configuration change.
- Never deploy production.
- Never deploy the normal TEST Worker unless explicitly instructed to deploy that exact Worker/environment.
- Use normal TEST directly for investigation of code already installed there.
- Use the isolated Codex Worker only for explicitly approved local-patch deployment/testing, with runtime freshness proof and TEST-only resources.
- Do not run migrations, destructive SQL, payment/provider/settlement/remittance/webhook/email/comms/background operations without explicit approval for the exact action.
- Keep CloudTMS Policy X intact for all Banking Pay/payment work.
- Never expose Wrangler OAuth data, environment variables, `.dev.vars`, Supabase credentials, database URLs, tokens, or sensitive payloads.

