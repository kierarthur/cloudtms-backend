# Native Windows PowerShell module-path successor

Frozen R1 auth package `ba01714404c0ff28adf02ceefb04bd6723206823` remains unchanged. Root's actual R1 stopped at the private-directory ACL child before browser, env loading or login: Windows PowerShell 5 inherited PowerShell 7's `PSModulePath`, so Security module import failed. The reported fresh folder remained empty. This successor neither removes nor reuses that folder.

The only runtime changes are three exact launcher hunks: this new package's location admission; a pure ACL-child environment builder; and the ACL-child invocation with scoped native PS5 `Modules` plus a closed ACL-phase HOLD. Every other auth/runtime helper, original source pin, admission rule, deadline and owner-only ACL script is byte-identical to R1. All 19 original package files are checked against saved/index/HEAD and immutable ba017 Git bytes. `LAUNCHER_FULL_PARENT_TRANSFER.json` reverses the complete current launcher to the captured raw R1 source. The original two harness/config parent inverses remain unchanged.

`aclChildEnvironment` clones the inherited environment, removes all case variants of `PSModulePath` (Windows keys are case-insensitive), and sets only that child's value to `Modules` beside the exact Windows PowerShell 5 executable. Parent/global/machine configuration, execution policy and the Playwright child's separate environment are unchanged. If ACL setup fails, the launcher returns only HOLD, `AUTH_PRIVATE_DIRECTORY_SETUP_HELD`, phase `PRIVATE_DIRECTORY_ACL`, its owned directory path and false browser/credential-loader start flags. No raw error or private value is returned.

Root command after independent review, from an actual physical D workspace and outside every native interval:

```powershell
node D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/docs/healthroster-build/proposals/test-auth-refresh-ps5-forward-20261017/run-auth-refresh.mjs --authorised-root-auth-refresh --outside-native-interval
```

The runtime still uses the original approved UI, stops for 2FA, loads the approved env only in its child after browser launch, requires actual ME200/admin UUID and exact queries-workspace200 before export, blocks redirects and business writes, allows exactly one login and at most one ordinary refresh, writes fresh private ignored I2 state, and confirms browser closure before success. It never replaces shared state. The standalone child still enables no test runner, auth screenshots, traces, videos, request/page logs or credential-bearing child output. Admission remains distinct from native WP-PP/fixtures/business currentness. The flags remain an operator assertion about native intervals.

The author executed a real native PS5 Security module import and **Get-Acl on this public proposal directory only**, with the same child environment builder. The probe resolves native Get-Acl/Set-Acl cmdlets but never invokes Set-Acl. Tests also exercise the actual complete launcher with a synthetic failing ACL child and prove no later copy/spawn/login is reached. No auth, browser, network, credential loading, private-state access, ACL mutation or native call was run by the author.

Offline/root review commands:

```powershell
python D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/docs/healthroster-build/proposals/test-auth-refresh-ps5-forward-20261017/verify-forward.py
node --experimental-vm-modules --test D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/docs/healthroster-build/proposals/test-auth-refresh-ps5-forward-20261017/test.mjs D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/docs/healthroster-build/proposals/test-auth-refresh-ps5-forward-20261017/test-ps5.mjs
```

The second command includes the explicitly bounded public PS5 module/ACL-metadata probe; it performs no authentication. Genuine root auth execution remains required. TLS, readiness, UI, 2FA, current authority, denied routes or later browser failure may still produce the original closed HOLD results. There is no automatic retry or cleanup fallback.
