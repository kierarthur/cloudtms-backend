# Arthur account TEST diagnostic connection — source evidence

This is a source snapshot, not an application deployment and not a change to the
shared lab. It records the code used by the separately authorised
`codex-arthurrai2006-miget-transaction-lab` Worker so its TEST deployment is not
dependent on uncommitted files in another worktree.

The two directories retain their original layout from backend `infra/miget/`.
Their saved source, tests, configuration and dependency lock are unchanged except
for normal Git LF line endings. No `.env`, `.dev.vars`, token, generated bundle,
generated type declaration, cache or `node_modules` is included. The primary
worktree and other accounts' connectors are untouched.

Only the configuration in `cloudtms-miget-transaction-lab-arthurrai2006` identifies
Arthur's deployment. The adjacent generic configuration is retained as source
evidence; this does NOT authorise redeploying that shared Worker. Rebuilding or
redeploying either connector remains a separate explicitly authorised action.
Do not rotate, copy between accounts or recreate an existing credential.

Arthur's authenticated HTTP route was used for the rollback diagnostics. Native
plugin pickup after a complete Codex restart remains unverified in this session.
The application/database release itself uses the protected GitHub workflow, not
this rollback-only diagnostic tool.
