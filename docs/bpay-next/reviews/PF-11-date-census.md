# PF-11 date and timestamp consumer census

Status: bounded source inventory complete; root's concurrent presentation changes are present in this checkpoint. Three finite history fields still require explicit routing in the bytes inspected. This is source review only, not a completed joined TEST result.

## Binding scope

- R3 plan `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:1899–1918`: PF-11 sequence 24, dependency F-16, UK London including BST, display `DD/MM/YYYY hh:mm:ss` in 24-hour time. Line 1908 requires one shared London timestamp formatter across NEXT Office/history and every timestamp consumer, plus the raw-ISO check. Line 1911 includes screens and exports. Database/Worker ownership and UTC storage stay unchanged (1909).
- `01_BUILDER_RULES_AND_FOUNDATION.md:23` B1.3 and card 1903: `TS_DAY` bucket remains `YYYY-MM-DD` (RQ252/SRQ231). It is a civil key, not an instant.
- `08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md:108,409`: sequence 24 / F-16 dependency. Build checklist rows `PF-11-D01`, `PF-11-D02`, `PF-11-D03` are respectively Office/history/exports formatting, raw ISO plus TS_DAY source check, and future joined in-app TEST checklist. No future-card scope or new financial owner is needed.
- The card's historical `banking-pay-reset-office-20261004/js/bpay-next-office.js` maps to current BUILD-FE `js/bpay-next-office.js`. The reconciled F-16 history module is `js/bpay-next-payment-history.js`. There is no product `js/bpay-next-office-frozen-detail.js`; frozen detail lives in Office. A similarly named ordinary unit file exists.

## Existing helpers and current shared implementation

`js/main.js:172341–172347` contains `formatUkDate`, the established exact date-only rearrangement. The card's historical 172147 line has moved. It does not construct a Date or change the civil day.

`main.js:172389–172403` has `formatUkTimestampFromUtc`: London date and hours/minutes, but no seconds and an `hrs` suffix. `main.js:379146–379209` and `379214–379344` contain the broader `formatCloudTmsLondonDate` and `formatCloudTmsLondonDateTime`: long month / prose output; timestamp display lacks seconds, and date-only input can be interpreted as a London midnight. These helpers are not the PF-11 exact format. No broad legacy-helper rewrite is required.

During this inventory root added `js/bpay-next-dates.js`. It exposes `formatDate`, `formatTimestamp`, `formatAdvice`. The timestamp formatter uses `Intl.DateTimeFormat` with explicit `Europe/London`, 2-digit day/month/hour/minute/second, numeric year, `hourCycle:'h23'`; accepts actual zoned ISO and PostgreSQL history representations, validates the civil component, drops display fractions, and returns `Not recorded` for unavailable or invalid/unzoned values. `formatDate` uses `root.formatUkDate` when exposed and otherwise performs the same exact civil rearrangement. Office/history import this one implementation; `index.html:3023–3026` includes it before owner/Office/history. Thus the originally observed UTC-slice implementations have already been replaced; they are not outstanding findings.

## Office consumers and preserved identities

All line numbers below refer to current `js/bpay-next-office.js`.

| Consumer | Current source | Formatting / required preservation |
|---|---|---|
| Current approved Work week | 540 `week_ending_date` | `humanDate = dates.formatDate` at 465. Preserve civil date and raw cursor. |
| Frozen line Work date | 587 `work_date` | Date-only helper. |
| Frozen shift date / instants | 589 `work_date`, `shift_start_at`, `shift_end_at` | Date-only plus `humanTimestamp = dates.formatTimestamp` at 466. |
| Frozen break instants | 590 `break_start_at`, `break_end_at` | Shared London timestamp helper. |
| Captured local shift/break clocks | 467,589–590 `*_local` | Already local clock facts. Keep local clock pair; never apply a timezone conversion or invent an instant. |
| Pay date input | 533 type=date; 535 CREATE payload | Input/wire remains `YYYY-MM-DD`. Current default is `new Date().toISOString().slice(0,10)` (UTC today); if the product requires London calendar today during the late BST rollover, change only that default with an explicit London civil-day derivation. This is not authority to shift any selected day or change payload notation. |
| Date / timestamp validation | 135,143 | Parsing/UTC round-trip is type validation, not presentation. Keep. |
| CURRENT_WORK pagination | 349 | Raw `[week_ending_date,work_id]` ordering and identity stay unchanged. |
| Original bank CSV download | 529/576 invoke download; 601 Blob of `result.csv_text` | Retain original CSV bytes, file_name, hash, captured bank-format semantics. This export is an issued bank instruction; UI display format is not authority to rewrite it. |
| Printable original frozen advice | 594→609–628 | Streams existing immutable advice HTML; 626 now calls `dates.formatAdvice(child.document)` after successful complete-marker check. Finite rendered date/time cells are reformatted in the display DOM, with exact frozen values retained in transport. |

Owner module does not render a timestamp/civil day. Its timeouts/TTL metadata are control values, not new UI date consumers. Office worker/case/return tables currently render no additional timestamp columns; no new table columns are needed for PF-11.

## History: complete finite temporal-field census

`js/bpay-next-payment-history.js:12–63` has 52 fixed sections. Current `displayScalar` at 277–283 dispatches timestamp suffixes `_at`/`_at_utc` and civil suffixes `_date`/`_monday`/`_week_start` through the shared helper. At 333 it is used for both summary key values and all rendered row.data values. No raw row/cursor/model conversion is needed.

**Outstanding source gap in inspected bytes:** `EXPIRY_REQUEST.captured_deadline` is a timestamp without the suffix; `POLICY.date_from` and `POLICY.date_to` are civil dates without the suffix. Add these three exact fields to existing dispatch, or use the equivalent fixed temporal set. They currently fall through as raw text. Do not classify arbitrary notes, evidence or identifiers by whether their contents resemble a date. In particular `candidate_id` is an identifier, never a date field.

| Section (literal source line) | Instant fields → London DD/MM/YYYY hh:mm:ss | Civil fields → DD/MM/YYYY; original TS_DAY stays YYYY-MM-DD |
|---|---|---|
| `WORKS` (12) | `created_at_utc`, `updated_at_utc` | `week_ending_date` |
| `REVISIONS` (13) | `approved_at_utc`, `sealed_at_utc`, `created_at_utc` | `week_ending_date` |
| `LINES` (14) | `created_at_utc` | `work_date` |
| `SHIFTS` (15) | `shift_start_at`, `shift_end_at` | `work_date` |
| `BREAKS` (16) | `break_start_at`, `break_end_at` |  |
| `POSITIONS` (19) | `updated_at_utc` |  |
| `COLLECTIONS` (20) | `created_at_utc`, `updated_at_utc` |  |
| `EFFECTS` (21) | `occurred_at_utc` |  |
| `PUBLICATIONS` (22) | `applied_at_utc` |  |
| `RUN_WORKS` (23) | `created_at_utc` |  |
| `RUN_POSITIONS` (24) | `captured_at_utc` |  |
| `HOLD` (26) | `created_at_utc`, `finished_at_utc` |  |
| `PROJECTIONS` (28) | `accepted_at_utc`, `retired_at_utc` |  |
| `TRANSFERS` (29) | `created_at_utc` |  |
| `OUTCOMES` (31) | `occurred_at_utc`, `received_at_utc` |  |
| `RETURN_CASH` (32) | `created_at_utc` |  |
| `CSV` (33) | `issued_at_utc` |  |
| `INTERNAL_RECEIPT` (34) | `occurred_at_utc`, `received_at_utc` |  |
| `EXPIRY` (36) | `created_at_utc`, `started_at_utc`, `finished_at_utc` |  |
| `CASES` (37) | `updated_at_utc` | `due_date` |
| `COMPONENTS` (38) | `updated_at_utc` |  |
| `RULES` (39) | `case_created_at_utc`, `original_funded_at_utc`, `captured_at_utc` | `schedule_start_monday`, `next_due_monday` |
| `CASE_EVENTS` (40) | `occurred_at_utc` | `pay_week_start` |
| `INSTRUCTIONS` (41) | `captured_at_utc` | `pay_week_start` |
| `CASE_HOLDS` (42) | `created_at_utc`, `finished_at_utc` | `pay_week_start` |
| `RESULTS` (43) | `created_at_utc` |  |
| `COMMAND` (45) | `created_at_utc`, `sealed_at_utc` |  |
| `JOBS` (46) | `available_at_utc`, `queue_wake_after_utc` |  |
| `NET_REQUEST` (47) | `accepted_at_utc` |  |
| `CANCEL_REQUEST` (51) | `started_at_utc`, `finished_at_utc` |  |
| `REISSUE_CANCEL` (52) | `accepted_at_utc`, `completed_at_utc` |  |
| `WRITE_OFF` (53) | `accepted_at_utc`, `completed_at_utc` |  |
| `CASE_CREATE` (54) | `accepted_at_utc` | `due_date`, `opening_pay_week_start` |
| `EXPIRY_REQUEST` (55) | `captured_deadline`, `accepted_at_utc`, `finished_at_utc` |  |
| `CASE` (56) | `updated_at_utc` | `due_date` |
| `PROTECTED_RECEIPT` (57) | `accepted_at_utc` |  |
| `SOURCE_DETAIL` (58) | `created_at_utc` |  |
| `POLICY` (59) |  | `date_from`, `date_to` |
| `WORKER_CASE_HOLDS` (62) | `created_at_utc`, `finished_at_utc` | `pay_week_start` |

History date/timestamp key validation at 98–101 and raw result/keyset identities must remain unchanged. History has no independent CSV/download/print/export function; its header explicitly excludes CSV download. Do not build a future export feature to satisfy this card.

## Print / export closure

The true printable source is BUILD-BE `broker/src/bpay-next-frozen-advice.js`, not a separate FE frozen-detail module. Its finite display consumers are:

| Renderer source line | Frozen value |
|---|---|
| 223 `workHeading` | `week_ending_date` |
| 260 | `header.pay_date` |
| 261 | `header.issued_at_utc` |
| 314 | `line.work_date` |
| 325 | `shift.work_date` |
| 326 | `shift.shift_start_at`, `shift.shift_end_at` |
| 336–338 break table | `break_start_at`, `break_end_at` |
| 327 and 338 local clock columns | `*_local`: preserve actual local facts |

The renderer's `pair` at 133 and `tableRow` at 228 escape raw frozen values. Its existing labels and fixed break headings are now the exact allowlist consumed by FE `dates.formatAdvice`; parent performs display DOM rewrites only. That closes Office's actual print path without SQL/Worker RPC/storage/payment changes. The helper renames recorded timestamp headings to London time while preserving local columns and duration-only rows. Keep stream markers, CSP, actor checks, template marker and escaping intact. Printing should occur only after the formatting step. Existing bank CSV stays unchanged because it represents the same immutable instruction rather than arbitrary presentation dates.

## Narrow source checks and verification status

- Reuse ordinary existing `tests/unit/bpay-next-office.test.cjs`, `bpay-next-office-frozen-detail.test.cjs`, `bpay-next-payment-history.test.cjs`. A timestamp assertion formerly expected `06/10/2026 13:04:05.123456 UTC` for PostgreSQL `2026-10-06 13:04:05.123456+00`; the exact London display should be `06/10/2026 14:04:05`. Existing date-only assertion `2026-10-04`→`04/10/2026` remains correct.
- The mandatory D02 source check should enumerate these actual finite display consumers (including the three non-suffix history fields and exact advice labels/headings) and require the shared formatter. It should preserve raw ISO validators/cursors/request fields, TS_DAY type=date fields, immutable CSV and local clock fields. No repository-wide date keyword ban, new proof framework or business fixture is necessary.
- Pure formatter cases should cover winter, BST, the exact midnight crossover `2026-06-30T23:30:00Z`→`01/07/2026 00:30:00`, both DST changes, PostgreSQL space/+00 notation, ISO offsets, seconds, absent values and unchanged civil dates. This inventory did not execute them.
- D03 stays a future joined in-app TEST checklist outcome. No app, scenario, business call, test, SQL or hosted operation ran in this inventory. Root may record normal source check results separately; do not infer a joined test result from this file.

## Source checkpoint hashes

These are observed saved bytes during concurrent root implementation, not baseline authority or a whole implementation PASS.

| Path | SHA-256 |
|---|---|
| `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\bpay-next-build-frontend-20261009\js\bpay-next-dates.js` | `d288271d326a076cedb431a801aff684cde7c126ffee0b804a3397a2809f29b2` |
| `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\bpay-next-build-frontend-20261009\js\bpay-next-office.js` | `d6f0a878ed356c5e23fe3121813928af130966611f967ba52c1829c8ab33e0f9` |
| `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\bpay-next-build-frontend-20261009\js\bpay-next-payment-history.js` | `1fa2b62a050a1859ae29c0152b0a1b9aa01619c6d1ea9f725c5076886d0ebed6` |
| `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\bpay-next-build-frontend-20261009\js\bpay-next-owner.js` | `4947d2e61cd4fa734cbaa99b05c415bad8552865d286233123cc48f34e1ca922` |
| `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\bpay-next-build-frontend-20261009\js\main.js` | `337ad43e351b88f8661169365c391fbca9e9d98cb2b877828df49aa7e2c2963c` |
| `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\bpay-next-build-frontend-20261009\index.html` | `4b854d032c9f37d4402d13f7b4e31b23634e436aceb37a14f3ad90d90ff28096` |
| `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\bpay-next-build-backend-20261009\broker\src\bpay-next-frozen-advice.js` | `010d06386af89983e48c63a3eafcc2796eba929f0aa74def33202157b55c00c4` |

## PF-11 three-field closure (fresh source follow-up)

The three finite history gaps above are now closed in current saved source. `js/bpay-next-payment-history.js:280` explicitly routes `captured_deadline` through the shared London timestamp formatter, and line 281 routes `date_from`/`date_to` through the date-only formatter. `tests/unit/bpay-next-payment-history.test.cjs:52–54` contains positive source cases for both civil fields and the exact BST midnight crossover. The earlier outstanding-gap paragraph records the prior checkpoint; it is no longer an outstanding finding. Source closure PASS for these three fields only; no test/app/runtime/SQL execution or joined TEST claim.

- Current history SHA-256: `1fa2b62a050a1859ae29c0152b0a1b9aa01619c6d1ea9f725c5076886d0ebed6`.
- Current history ordinary unit SHA-256: `8c9fef76951058926112c6263a49686b22a9f61361ca17f8d360cfa32c6b1b3f`.

## PF-11 JOBS timestamp closure (second fresh follow-up)

The complete temporal census now explicitly includes `JOBS.queue_wake_after_utc` (history section literal line 46), a timestamptz whose name does not end `_at_utc`. Current dispatch at 280 formats it alongside `captured_deadline`; ordinary positive source case at unit line 55 uses PostgreSQL fractional timestamp/offset notation and expects the exact London BST midnight crossover. The JOBS table entry above is corrected. Source closure PASS for this additional finite field; no execution or joined result. Current history SHA-256 `b81e0968410a2c6cc5500da50283d911c4ec603bc5b85f2b86de95b3d14cd258`; ordinary unit SHA-256 `a5f8cfd186accc30b079a85f487e3738f8afaabd3d674d1fbe842024714c875a`.
