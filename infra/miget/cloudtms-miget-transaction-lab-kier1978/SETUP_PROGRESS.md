# Kier1978 connector setup — 14 October 2026

HealthRoster implementation remains paused. This separate connector setup was explicitly authorised.

Hosted rollback acceptance passed. Separate shared lock taken 2026-10-14T20:51:10.274Z and released 2026-10-14T20:51:11.200Z; explicit unlock and connection close both true. No business data fixture or durable database change. Full Codex restart/new task remains necessary for native tool pickup.

## Native pickup — 15 October 2026

HealthRoster has resumed. The exact Kier1978 native tool is now callable in its task. Seven statements created and exercised the uniquely named acceptance table/row/function; accepted/physical agency TEST/rollback/fresh-connection absence all passed. SEND7 turn held00:50:26.295Z–00:50:43.765Z, explicit unlock and connection close true. The first noninteractive holder closed on stdin EOF before any trial; the PTY holder then spanned the actual native call and fresh absence proof. See NATIVE_TOOL_ACCEPTANCE_20261015.json. No deployment, credential change, other-account route, business fixture or external effect. Account AGENTS section records actual native proof. The historical missing bearer observation concerned an exec child, not directly the native MCP process; no credential replacement was needed.
