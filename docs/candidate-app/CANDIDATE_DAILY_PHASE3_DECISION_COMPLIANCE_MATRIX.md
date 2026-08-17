# Candidate Daily Phase 3 decision compliance matrix

Date: 17 August 2026

| ID | Atomic decision | Implementation evidence | Result |
| --- | --- | --- | --- |
| AV-250 | Missing/false bridge flag must be indistinguishable from the certified legacy path. | Both helpers return before network/log/state/Sheet work; executable false/missing matrix; guarded `typeof` seams. | PASS IN SOURCE |
| AV-251 | Do not modernise the temporary legacy browser; contain it behind Apps Script. | No browser, login, token, msisdn or UI source changed. | PASS |
| AV-252 | The security boundary is trusted Apps Script to CloudTMS Worker, never legacy browser to CloudTMS/Supabase. | Signed helper only; no browser credential or Supabase client. | PASS |
| AV-253 | Legacy identity is mapped from exact Credentially public ID to a non-reversible environment-bound source HMAC. | Identical source canonicalisation in both helpers; independent HMAC test. | PASS |
| AV-254 | Existing legacy action/result remains first and unchanged. | Availability legacy result is built first; Master captures `_postWithRetry` result before mirror and returns it. | PASS IN SOURCE |
| AV-255 | CloudTMS bridge failure must fail open toward the existing legacy response. | Helper catches and bounded logs; response seams return original envelope; helper-missing guards. | PASS IN SOURCE |
| AV-256 | Enabled legacy tiles use canonical CloudTMS/Supabase tile truth without losing legacy-only fields. | Signed Worker read; by-date canonical merge over existing envelope; fallback unchanged. | PASS IN SOURCE; LIVE TEST LATER |
| AV-257 | Master Rota continues Availability publication and adds CloudTMS generation publication. | Existing call remains first; only update-end mirrors. | PASS IN SOURCE |
| AV-258 | Every published generation contains exactly 14 valid days and at most 50 candidates per batch. | Builder rejects non-14 window, hashes rows/items and chunks by 50; unit test. | PASS |
| AV-259 | Availability and generation commands retain one durable identity across uncertainty. | Script Properties plus Script Lock freeze request/batch, key, body and correlation. | PASS IN SOURCE |
| AV-260 | Availability lost-response recovery is status-first and consumes at most one exact retry. | `retry_consumed` state; executable network/404/lost-retry/status-only test. | PASS |
| AV-261 | Apps Script signing must match frozen HMAC v1 bytes exactly. | Frozen UTF-8 vector signature matches. | PASS |
| AV-262 | Secrets and raw candidate identity must not enter source, payloads or bridge logs. | Property-only secrets; source-HMAC payload; structured bounded logs; source scan/test. | PASS |
| AV-263 | Projection delivery must preserve booked/system-blocked overlays and report lease outcomes. | Explicit claim/complete adapter; deferred overlay or existing mapping only. | PASS IN SOURCE; SCHEDULING LATER |
| AV-264 | Effect authority is receipt-first; Phase 3 must not invoke providers merely by installing the adapter. | Claim/complete/status primitives only; no action wiring or trigger. | PASS |
| AV-265 | Existing trigger inventory and orphan `ai_startDailyPings` state must remain unchanged. | No trigger creation; no function declaration or cleanup; executable static test. | PASS |
| AV-266 | Availability, Emergency and Master Rota survive temporary legacy-browser retirement until separately migrated. | Architecture and decisions explicitly retain them. | PASS IN DESIGN |
| AV-267 | Installation and rollback must be complete, unredacted and copy/paste-ready. | Full revised Code/helper files plus byte-exact certified rollback files and SHA-256. | PASS |
| AV-268 | Phase 3 source delivery does not enable Candidate Daily or mutate Candidate data. | No live Google edit, Supabase mutation, flag/entitlement change or external effect. | PASS |
| AV-269 | Google installation, real signed route parity, quota/latency soak and outage/recovery remain independent acceptance gates. | Runbook and review brief mark them unexecuted. | PASS |
| AV-270 | No Office/finance/invoice/payment/Banking Pay/Policy X authority may drift. | Changed boundary is Apps Script package, test and Candidate documentation only. | PASS |
| AV-271 | Deferred rota-busy rows are non-authoritative and cause no CloudTMS identity, state, log or request before flush. | Busy branch has no mirror call; executable source-seam and false-work tests. | PASS |
| AV-272 | A CloudTMS Availability command contains only rows durably accepted by the legacy write. | `ctmsP3_appliedLegacyChanges_` derives the body from `legacyResults`; all-accepted, all-rejected and mixed-result tests. | PASS |
| AV-273 | Zero durable accepted rows is a true no-op before identity/state/log/network work. | Early return precedes source lookup and operation creation; executable all-rejected test. | PASS |
| AV-274 | Queued rows mirror only after successful value/background writes and after the legacy write lock is released. | `_flushPendingWrites()` builds a post-write subset; executable lock/write/mirror ordering test. | PASS |
| AV-275 | HTTP status alone never classifies an Availability failure as terminal. | Closed route-specific `http/error_code/retry_class` catalogue replaces the broad 4xx classifier. | PASS |
| AV-276 | `COMMAND_IN_PROGRESS`, `SOURCE_IDENTITY_NOT_READY` and `IDENTITY_LINK_MISSING` retain the exact operation and invoke status. | Executable matrix asserts no clear, retained state and exact status route. | PASS |
| AV-277 | Only approved `DO_NOT_RETRY`/`REFRESH` triples clear as terminal rejections; malformed 4xx remains uncertain. | Executable terminal/malformed-409 matrix. | PASS |
| AV-278 | Authoritative status not-found permits one exact retry only; later not-found is status-only. | Full lost-response test compares body, key, correlation and source identity and caps execute calls at two. | PASS |
| AV-279 | The audit archive must be self-contained for the supplied Phase 3 test. | R12 pack includes packaged source-path fallback and the frozen canonicalization fixture; validator executes the test from extraction. | PASS |
| AV-280 | R12 does not broaden into Master Rota semantics, Worker/database contracts, projections/effects, legacy UI, Emergency, finance or production. | Exact changed-file boundary and regression suite. | PASS |

## Pre-existing decisions

All accepted Sections 1–88 and AV-001–AV-249 remain controlling. AV-250–AV-280 are additive Phase 3 decisions and do not activate Candidate Daily or retire any legacy system. AV-271–AV-280 are later-controlling where R11 wording could otherwise permit mirroring deferred/rejected rows or clearing a non-terminal recovery operation.
