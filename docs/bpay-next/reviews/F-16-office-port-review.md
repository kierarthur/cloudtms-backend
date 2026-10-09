# F-16 independent Office port source review - 9 October 2026

**PASS SOURCE.** Freshly re-read saved BUILD-FE after root's Retry owner correction. No open source defect remains in the reviewed port. Installed/App tested remain NOT_RUN. This reviewer performed no tests, browser, app/runtime, SQL, fixture, hosted or network operations; root alone wrote product.

Authorities: F-16 Rules/Build/Done and exact checklist rows D01-D07, current owner start sheet, saved OFFICE originals, F-16 port design/inventory and fresh frontend base `aada67455afe5a3b6828a713c7062832c0617782`.

## Resolved finding

Retry exact request was initially enabled while ownership was unavailable/paused. Root now includes ownerReady failure in this button's busy predicate. Independently verified in the freshest source. Pending itself does not disable identical retry; its actor-namespaced exact bytes remain retained. Transport still refuses before network when the optional owner callback is false.

## Verified source behavior

- All 22 non-main files in the 23-path upstream preservation set retain their exact raw SHA256. Main contains only the bounded semantic owner/streaming/navigation/direct-open/logout/history changes; current Source/pay-method, Candidate, Contract/MyTMS, invoice and shared modal owners survive. Root restored original mixed line endings. New includes preserve every newer upstream marker and the current main include URL.
- Static order is owner helper, thin Office, history, then existing Banking/main; CSS is adjacent to existing Banking CSS. Exact new asset marker is `20261009-next-foundation-r1`. No dynamic load guess.
- Exactly one explicit owner begin per Banking menu open; pending reads coalesce. Records reuse the active Banking scope or qualify once per frame. Owner response is streamed with a 1024-byte ceiling, fatal UTF-8, eight-second abort, exact two literal JSON fields, closed owner enum and positive decimal-string epoch bounded to PostgreSQL bigint. Generation/scope/actor checks reject late answers; reset/close abort old reads. Failed record qualification cannot loop because its frame flag is set before requesting.
- NEXT navigation begins hidden/disabled and requires positive NEXT. Unknown, DISABLED and LEGACY show deliberate unavailable/paused/inactive copy. Both actual UI open methods require callable positively true ownerReady. Admin/View/genuine record UUID plus NEXT gate history; ordinary records fetch no history. History now receives BROKER_BASE_URL string; Office retains callable API. Library createClient/transport do not acquire a mandatory UI dependency.
- Legacy payment destinations/direct openBanking show deliberate unavailable copy; unrelated Invoice Discounting survives. OR-45 does not require old-engine operation. No financial recalculation was added to opening.
- Frontend split/filter/join pathname normalization matches backend duplicate/trailing slash normalization. Exact exemption is POST command/preparation/history-page/frozen-detail, GET preparation-page/owner-status, GET advice plus one UUID. Method mismatch, unknown suffix and malformed advice do not qualify; query strings preserve route identity. Existing debug clone/text/log buffering is suppressed only for this exact route set.
- Closed error shapes precede handling of actual `BPAY_NEXT_COMMAND_MODULE_NOT_ACTIVE` and `BPAY_NEXT_PREPARATION_MODULE_NOT_ACTIVE`. These alone invalidate and re-read owner state; safe refusal preserves ownerCode. No automatic mutation replay. Cache clear leaves accepted financial state/journal intact.
- Journal v3 keeps actor-specific exact action bytes/UUIDs before dispatch and narrow old-version adoption. Definitive initial refusal may clear that initial pending action; refusal of a retry after an earlier unknown outcome preserves identical pending bytes until a validated receipt. Owner/actor refusal, sheet close and logout also retain the journal. Sheet close removes progress timer/owner listener; render epoch rejects closed-sheet late updates.
- clearSession closes Office/history client, then explicitly pops only a current banking-pay-history frame and closes/clears the owner helper before existing session cleanup. Aborted client/page clear plus visible frame removal jointly close history while preserving unrelated modal owners.
- Visible LOAN values map to Payment Advance in Office case/component tables and history case-kind/subtype/advance-kind fields; wire values are unchanged. Escaping/textContent, decimal/null distinction, captured frozen amounts/rates, SOURCE/PROTECTED_WORK/WITHDRAWN facts, CSS/Escape/focus/dark rules and factual paging survive. No payment total or price calculation is added. This thin port does not certify later full Workbench/Draft/Execute/Finance/Payment Summary screens or OR44-47 feature completion.

## OFFICE untracked unit classification

| Source | Decision |
| --- | --- |
| tests/unit/bpay-next-office.test.cjs | Port normal node:test controller/transport/source units; update obsolete exact asset-marker assertions only. |
| tests/unit/bpay-next-office-frozen-detail.test.cjs | Port normal node:test actual transport/handler and SQL-source assertions; rebind explicit RESET backend sibling path to BUILD-BE. No SQL execution. |
| tests/unit/bpay-next-payment-history.test.cjs | Port normal node:test validator/controller/source units; rebind RESET import to BUILD-BE and current history include marker. |
| tests/unit/bpay-next-payment-history.browser.test.cjs | Exclude: imports Playwright, launches Chromium and loads tests/fixtures HTML. |

Root ported only the three source units with backend path/current-marker adaptation and reported 60/60 PASS plus syntax PASS in BUILD-BE `.codex-local/F-16-source-gates.log`. Reviewer did not execute/re-certify that run. No browser fixture, runner, artifact directory, login state or secret was ported or opened by this reviewer.

## Current saved raw hashes

| Path | Raw bytes | SHA256 |
| --- | ---: | --- |
| `js/bpay-next-owner.js` | 5785 | `4947d2e61cd4fa734cbaa99b05c415bad8552865d286233123cc48f34e1ca922` |
| `js/bpay-next-office.js` | 80889 | `a4ba8ea0b63f3723a062dea7f915e91ed193c3c20c03e9dfae7765e8de25a356` |
| `js/bpay-next-payment-history.js` | 43925 | `715a1035f88204b39771932a479f733b98ef7c971656c77767bd396861a24e9b` |
| `css/bpay-next-office.css` | 1997 | `5fa3e52ab734f0fee7071d364f57c44e7130478648fe797d98bf3b6804b50b1f` |
| `js/main.js` | 17755831 | `337ad43e351b88f8661169365c391fbca9e9d98cb2b877828df49aa7e2c2963c` |
| `index.html` | 100185 | `608de5cecc565a7d22071059de48978655c2b2554fb223f8d85d577d7e91bd78` |
| `BANKING_PAY_BIBLE.md` | 147544 | `7ae0749e1926b25407364944c9611713cae017d4786e2b67ea82d6778d526644` |

Exact per-path preservation and unit-source hashes are in `F-16-office-port-review.json` beside this file. Source review only; installed/app acceptance NOT_RUN.
