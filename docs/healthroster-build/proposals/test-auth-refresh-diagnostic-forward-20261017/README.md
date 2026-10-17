# Closed startup-write diagnostic

R1 ba017 and PS5 successor da869 remain unchanged. Root's actual da869 run closed its browser and returned `AUTH_NETWORK_BOUNDARY_HELD`, login1/refresh0/blocked reads6/forbidden writes1/refused0, with no state file. Those counters **do not identify the method or route**. No credential failure or observed ME/workspace success is inferred.

This finite successor preserves every route decision, request cap, redirect refusal, child-only env boundary, PS5 ACL fix, genuine admission and state-export condition. Four runtime files add closed diagnostics only; `DIAGNOSTIC_FULL_PARENT_TRANSFERS.json` supplies complete R2 parent captures/inverses for them and the adapted test. Unchanged credential/admission/config/ACL/pin files are selected exactly from da869. Original R2 helper tests/inverses remain in that immutable package; the new bundle contains only needed runtime sources and its own proof.

The blocked-write diagnostic contains a closed method enum (`NONE`, `POST`, `PATCH`, `PUT`, `DELETE`, `OTHER`, `MULTIPLE`) and a closed route-label enum. `STATIC_CHANGES_PING` derives from exact public Frontend `/api/changes/ping` POST source; `STATIC_USER_GRID_PREFS` derives from exact `/api/users/me/grid-prefs` PATCH source. Other normal-backend writes map to `OTHER_NORMAL_BACKEND`; different labels/methods aggregate to `MULTIPLE`. These labels are **diagnostic only**: every such write remains aborted and makes admission fail. They never permit heartbeat or user-preference mutation. Neither static candidate is claimed to be the R2 request.

Only URL origin/path transiently select a fixed enum. No raw URL/path/query, UUID, cookie, body, user field or hash of a request URL is retained or printed. `STATIC_ROUTE_SOURCE.json` pins complete public `js/main.js` saved/index/committed raw bytes plus only literal-call/method spans. Runtime checks its saved hash and pinned Git blob before any credential loading; it never copies the public source into private auth output.

After the actual existing admission function returns, the callback records only its six exact fields: ME/workspace integer statuses and actor-UUID/admin/contract/queries booleans. Earlier failure produces `BEFORE_EXPLICIT_ADMISSION` with `admission:null`; returned observations produce `EXPLICIT_ADMISSION_RETURNED`. A local closed result V2 adds this strict diagnostic object. The parent forwards that observed object on HOLD/success, or null when no closed child result is available. No inferred control-flow observation fills missing fields. No raw actor UUID or user profile leaves the page.

State export still requires actual ME200/admin UUID, exact queries-workspace200, original network admission, and fresh private owned state. Browser closure remains mandatory for success. One login/at most one refresh, normal TEST hosts, blocked business methods, 2FA stop, no capture/logs, child environment restoration, native-interval operator flags and timeout/cleanup bounds are unchanged. This changes no application endpoint, auth protocol, product code or global configuration.

After root review, from an actual D workspace outside every native interval:

```powershell
node D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/docs/healthroster-build/proposals/test-auth-refresh-diagnostic-forward-20261017/run-auth-refresh.mjs --authorised-root-auth-refresh --outside-native-interval
```

Source checks:

```powershell
python D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/docs/healthroster-build/proposals/test-auth-refresh-diagnostic-forward-20261017/verify-diagnostic.py
node --experimental-vm-modules --test D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/docs/healthroster-build/proposals/test-auth-refresh-diagnostic-forward-20261017/test.mjs D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/docs/healthroster-build/proposals/test-auth-refresh-diagnostic-forward-20261017/test-diagnostic.mjs
```

Fifteen tests include original controls, actual callback HOLD/no-export evidence, closed observation validation, URL/ID canaries, 550 old/new route/cap decisions and exact unchanged five counters. The author ran no auth, browser, network, ACL mutation or credential/state inspection. Genuine root execution remains required; no success is claimed in advance.
