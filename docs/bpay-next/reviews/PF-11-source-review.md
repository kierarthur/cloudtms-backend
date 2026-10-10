# PF-11 D02/D03 independent source review

10 October 2026. Verdict: **PASS for D02's finite consumer/source checks and D03's future joined checklist.** Read-only review of current BUILD-FE and BUILD-BE saved sources against full PF-11 card `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:1899–1918` and its three current checklist rows. No product edits, tests, runtime, browser, SQL, data, hosted or timing operations were performed by the reviewer. Actual installed/browser/joined acceptance is not inferred.

| Source | SHA-256 |
| --- | --- |
| FE `tests/unit/bpay-next-payment-history.test.cjs` | `a5f8cfd186accc30b079a85f487e3738f8afaabd3d674d1fbe842024714c875a` |
| FE `js/bpay-next-dates.js` | `d288271d326a076cedb431a801aff684cde7c126ffee0b804a3397a2809f29b2` |
| FE `js/bpay-next-office.js` | `d6f0a878ed356c5e23fe3121813928af130966611f967ba52c1829c8ab33e0f9` |
| FE `js/bpay-next-payment-history.js` | `b81e0968410a2c6cc5500da50283d911c4ec603bc5b85f2b86de95b3d14cd258` |
| FE `index.html` | `4b854d032c9f37d4402d13f7b4e31b23634e436aceb37a14f3ad90d90ff28096` |
| BE `docs/bpay-next/design/PF-11-SCOPE.md` | `2e36b20b16b0031b84d79575e72440a5cc6c3111c9f17feef0277d473c8ed435` |
| BE `docs/bpay-next/PF-11-JOINED-ACCEPTANCE.md` | `710d73de04d3cd165b2a85a2e7dbeec91a424d692fb6bbe887a77a2c2811ca33` |

## D02 consumer and negative-control coverage

The existing unit file now checks actual shared helper, Office, history and HTML source rather than merely finding an unused formatter. The shared module is present before both consumers; actual assets share `20261010-next-dates-r1`. `main.js`'s established `formatUkDate` remains the date-only helper at line 172341, accessed at presentation time, with a matching date-only fallback for normal module tests. There is one shared Europe/London `Intl.DateTimeFormat` with `hourCycle:'h23'` and second precision.

The current Office Work/week and frozen line/shift dates call `humanDate`; all four recorded shift/break endpoints call `humanTimestamp`. Local duration-only clocks remain `clockPair` facts. History's generic scalar display routes the timestamp suffixes through the shared timestamp formatter and explicitly includes `captured_deadline` and JOBS `queue_wake_after_utc`; date-only suffixes plus POLICY `date_from`/`date_to` use date-only formatting. These routes also serve the visible key summaries. No raw-ISO slicing or UTC-label presentation remains in the reviewed consumer seams.

The same finite check is run against seven in-memory source mutants: raw timestamp alias, raw break endpoint, raw history timestamp, UTC replacing London, missing print formatter, a formatted CSV Blob operand, and a missing shared dependency. Each mutant changes an actual current seam. Positive checks retain the current source and explicitly exclude formatting from request-body and history request/cursor validators. This is a meaningful bounded source regression check in the existing unit file, not a new browser harness or scenario programme.

The two adjacent normal cases exercise summer/winter instants, both BST transitions, equivalent explicit-zone ISO/PostgreSQL strings, rejected invalid/unzoned timestamps, date-only formatting, captured deadline, queue wake and policy dates. Printable-fact controls preserve null/duration-only endpoints, local labels, exact large decimal text and timestamp-shaped free text in Explanation. Existing contract, cursor, escaping, dormant-client and framework cases remain; only the superseded UTC expected display and asset marker were updated to the required current contract.

## Wire and original execution artifact

Presentation conversion operates on strings supplied to rendered cells; it does not mutate captured rows, date buckets, IDs, monetary operands, query arguments or cursor tuples. The source check excludes formatting from the real mutation/request and history-validation seams. TS_DAY remains YYYY-MM-DD under Policy X; this card does not invent a NEXT TS_DAY mutation or a new temporal authority.

The existing Worker frozen-advice renderer's exact labels match the shared formatter's finite label map: Pay date, Week ending, Work date, issued/recorded timestamps, and recorded break columns. It formats only the completed child document. Local clock labels, money, free text and the duration-only explanatory row are preserved. The original bank CSV still downloads with `new Blob([result.csv_text])` and its original filename; no shared formatter is invoked on those bytes. Stored original execution artifacts and all DB/Worker owners remain unchanged by PF-11.

## D03 scope

The future joined note covers actually served/coherent assets, the mandated 30 June 23:30 UTC example, winter/BST/DST display, date-only/wire identity, frozen endpoint/print consistency, original CSV bytes, retained financial meanings and failure/actor behavior. Every browser/full Worker/queue/minute clause is explicitly NOT_RUN. Source cases are not presented as served-asset parity, browser formatting, real data, joined acceptance or publication evidence. Stored Workbench checks require no completed UI, and parked accepted-cut/resolver work remains unused.

Fresh final follow-up: independently re-read the explicit `queue_wake_after_utc` display route and its actual PostgreSQL-zone timestamp positive case. This closes the non-suffix instant alongside `captured_deadline`; `date_from`/`date_to` remain explicit date-only cases. The two updated hashes in the table identify this final source, superseding the earlier test `8c9fef76…` and history `1fa2b62a…` hashes.

No open concrete finding remains for D02/D03. Root reports the complete existing source suite 63/63 and two syntax checks passing after the final edits; the reviewer independently read the saved code/cases and did not execute them. D01's full product review and later joined TEST evidence keep their separate authorities.
