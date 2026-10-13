# PF-11 format implementation source review

10 October 2026. Independent saved-source review of the full PF-11 card (06:1899–1918), D01–D03 scope in design/PF-11-SCOPE.md, BD028, frontend desktop override, new shared formatter, both actual Office consumers, index dependency order, changed ordinary source units and the unchanged original advice renderer. No product edits, test execution, browser/scenario calls, SQL or runtime operations by this reviewer.

Final verdict: PASS for the bounded D01 implementation and D02 source routing after the correction recorded below. Initial finding: JOBS.queue_wake_after_utc is an actual timestamp, but the first reviewed displayScalar routed only _at/_at_utc and captured_deadline, leaving this field raw in visible history. The exact closed field appears in frontend history line46 and backend history line47; history1206 line104 maps it to private.bpay_next_job. Migration04102026_0502 line6 declares it timestamptz. Root corrected only this exact presentation field and its ordinary positive check; no SQL or Worker change was required.

The other bounded changes pass source review:

- The one Intl.DateTimeFormat uses Europe/London and hourCycle h23. Explicit zoned ISO and PostgreSQL text are normalized before instant parsing; unzoned/invalid timestamps are refused to Not recorded. Seconds are displayed without changing stored fractional precision or transport. Summer date rollover and DST conversion follow the actual London instant; the reviewed ordinary unit expectations cover both DST transitions and winter/BST.
- Date-only presentation reuses actual main.js formatUkDate (line172341) when loaded; its fallback has the same YYYY-MM-DD rearrangement. No timestamp conversion is applied to date-only WORK/TS_DAY buckets, request fields, continuation keys, cursor comparisons or journal bytes. Date input value remains the required YYYY-MM-DD browser/wire representation.
- Office displayed week/work dates and four recorded shift/break endpoints route through the shared helper. History row summaries and detail values both route through displayScalar; captured_deadline and policy date_from/date_to are included in the latest reviewed source. Local clocks and duration-only records remain their recorded values; no endpoint or timezone is inferred.
- Streamed advice labels and table headings match the unchanged backend renderer exactly: Pay date, Week ending, Work date, Issued at, Recorded start/end and the two recorded break columns. DOM formatting runs only after the original complete-marker check and document.close, and completed becomes true only after formatting succeeds. Existing actor checks, authenticated route, CSP, opener removal, fatal UTF-8 decoding, bounded writes, cancellation and INCOMPLETE failure handling remain. Formatting writes textContent only and leaves money, free text, frozen IDs, local clocks, rate/quantity evidence and explanatory colspan rows untouched.
- Frozen bank CSV stays new Blob([result.csv_text]) with the original filename; no CSV reformatting or monetary calculation is introduced. Dates, owner, Office and history scripts use one coherent NEXT cache marker, with the shared dependency loaded before both consumers. The later main.js declaration is looked up at formatDate call time.

D02 ordinary source additions cover the explicit four recorded Office endpoints, shared London/h23 formatter, date-only request separation, print routing, bank CSV preservation and removed/rerouted-consumer refusals. The missing queue_wake_after_utc positive check above is the only requested correction. Parent-reported 62/62 ordinary checks are not an independent execution receipt. D03 joined browser/print/CSV and application timing remain NOT_RUN; this source review does not tick those later acceptance scenarios.

Reviewed SHA-256 (before the queue-wake correction):

| Frontend path | SHA-256 |
|---|---|
| js/bpay-next-dates.js | d288271d326a076cedb431a801aff684cde7c126ffee0b804a3397a2809f29b2 |
| js/bpay-next-office.js | d6f0a878ed356c5e23fe3121813928af130966611f967ba52c1829c8ab33e0f9 |
| js/bpay-next-payment-history.js | 1fa2b62a050a1859ae29c0152b0a1b9aa01619c6d1ea9f725c5076886d0ebed6 |
| index.html | 4b854d032c9f37d4402d13f7b4e31b23634e436aceb37a14f3ad90d90ff28096 |
| tests/unit/bpay-next-office.test.cjs | e5b406c49ee9a5f20cbcc10d82dd24e24b12297d5fc3ed9056ad8ae018b068b2 |
| tests/unit/bpay-next-payment-history.test.cjs | 8c9fef76951058926112c6263a49686b22a9f61361ca17f8d360cfa32c6b1b3f |

Fresh closure: re-read the saved complete history diff and ordinary-unit diff after root's correction. displayScalar now explicitly routes both captured_deadline and queue_wake_after_utc through dates.formatTimestamp. The added check uses the actual PostgreSQL zoned fractional-seconds form and expects London BST with date rollover. No remaining actionable formatting finding in the scoped current consumers. The shared helper, Office, index and Office-unit hashes above are unchanged. Latest history SHA-256 is b81e0968410a2c6cc5500da50283d911c4ec603bc5b85f2b86de95b3d14cd258; latest history-unit SHA-256 is a5f8cfd186accc30b079a85f487e3738f8afaabd3d674d1fbe842024714c875a. Root reports 63 ordinary checks and two syntax checks passing after correction; reviewer performed source reads only. D03 joined application acceptance remains outside this implementation review and NOT_RUN.
