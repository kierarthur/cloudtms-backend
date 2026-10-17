# One expected aborted startup heartbeat

Root's actual aaf6 diagnostic R3 observed ME200/admin UUID and workspace200/exact contract/queries, then held on one forbidden write: closed label `STATIC_CHANGES_PING`, method `POST`. Browser closed; no state was exported. Earlier R1/R2/R3 packets and results remain unchanged. This successor implements the root's explicit finite suppression authority; it does not call or grant permission to the ping handler.

Only exact queryless `POST https://test-cloudtms-backend.kier-88a.workers.dev/api/changes/ping` is classified `SUPPRESSED_STARTUP`, maximum one. The network guard **returns false and the unchanged transport aborts it before fetch**. No request is forwarded, body read or response fabricated. Its separate `suppressedStartupRequests` counter replaces the forbidden-write count only for that exact event. A second such event marks violation; unknown writes, grid-preference PATCH, alternate method/origin/path/query/fragment/encoding, redirects, auth caps and overall budgets retain their previous behavior. Static endpoint labels remain diagnostics, not write permissions.

The actual approved login UI, 2FA stop, genuine six-field admission, private child-only env, scoped PS5 ACL child, blank initial browser state, no captures/logs, and new ignored owner-only state are unchanged. State may be retained after one expected abort only when all genuine admission conditions pass. The worker rechecks network admission after confirmed browser close, before keeping state, so a late second ping or other forbidden callback yields HOLD and removes that new state. The parent independently requires the actual suppression counter≤1. Local closed result V3 adds only this nonnegative counter; observed admission/closed labels stay as in R3. No server endpoint/auth protocol/product code/global policy changes.

`SUPPRESSION_FULL_PARENT_TRANSFERS.json` contains three complete R3 runtime parents/inverses and two adapted test parents. Unchanged files/pins remain byte-identical to aaf6. Tests prove one exact classification difference while all sends remain rejected, all near misses remain unchanged, no ping fetch/response/body access occurs, and the complete actual worker closes and cleans up after late violations using only synthetic modules. No auth/browser/network/ACL mutation or private data read was executed by the author.

Root command after review, from an actual D workspace and outside every native interval:

```powershell
node D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/docs/healthroster-build/proposals/test-auth-refresh-suppressed-startup-forward-20261017/run-auth-refresh.mjs --authorised-root-auth-refresh --outside-native-interval
```

Source verification:

```powershell
python D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/docs/healthroster-build/proposals/test-auth-refresh-suppressed-startup-forward-20261017/verify-suppression.py
node --experimental-vm-modules --test D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/docs/healthroster-build/proposals/test-auth-refresh-suppressed-startup-forward-20261017/test.mjs D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/docs/healthroster-build/proposals/test-auth-refresh-suppressed-startup-forward-20261017/test-diagnostic.mjs D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/docs/healthroster-build/proposals/test-auth-refresh-suppressed-startup-forward-20261017/test-suppression.mjs
```

Nineteen tests pass. Genuine root login, browser closure, state export and subsequent use remain required. A further write or missing actual admission is HOLD; this source does not predict success or permit a retry bypass. No old private output directory is removed or reused.
