# PS-11 FE / history caller source review

**PASS — reviewed saved source; no actionable defect found in this slice.** Independent read-only review,10 October2026. No tests, SQL, runtime, fixtures, hosted calls or product edits performed. Root reports13 existing FE cases passed; that execution is not repeated or independently inferred here. App/installed/full Worker/queue/minute acceptance remains NOT_RUN.

Reviewed exact PS-11 card/checklistD01–D05, BD037 scope and future joined note; current FE history module/asset/Bible/existing test changes; matching BE classifier/caller tests; actual0380 Case NET schema and current0623 controlling closure. Rule-B and current52-section SQL/route/cursor/security contracts are preserved.

## Source evidence

* FE and BE `classifyBpayNextNet` have matching finite logic. Current PAYE NULL becomes NOT_ENTERED; recorded PAYE projection/request NULL refuses; exact nonnegative money text with an integer or one/two decimal places—including `65`, `65.0`, `0` and `0.00`—stays unchanged. Umbrella and genuine CASE_PAYOUT NULL become NOT_APPLICABLE. NET_REQUEST accepts only the actual schema's PAYE_MANUAL/CASE_PAYOUT kinds, while projection supports recordedPAYE_MANUAL/PAYE_IMPORT and actualnot-applicable kinds. No floating conversion, recovery subtraction, member aggregation or ratio appears.
* The original investigation's blanket NET_REQUEST NOT NULL statement was incomplete: actual migration0380:7–19 permits **CASE_PAYOUT with NULL NET and zero frozen gross**. BD037 and implemented classifier correctly use that real authority; this is not a new nullable financial input or guessed producer.
* Existing response validation invokes classification only after its closed field/scalar/key/scope checks. RUN_LINES/RUN_WORKS/LINES/SHIFTS remain intrinsically UNALLOCATED and reject added NET/cash allocation fields. There is no invented line NET field, conversion from earnings basis or new wire section.
* FE `displayScalar` receives the actual section and same row context at the existing renderer. “NET not entered” is reserved for current PAYE NULL; “PAYE NET not applicable” uses actual recordedkind. Exact zero remains `0.00`. Line amount notices distinguish unallocated perTimesheet/Client NET even when Candidate NET is recorded and explain the before-deductions workings boundary.
* `netContext` checks the current actor, exact Candidate manifest, native transfer/worker IDs and object identity against the actual validated retained transfer cache. Projection is fetched **only by that transfer.projection_id**, then checked for exact projection/worker identity. Frozen transfer/projection caches remain bounded128 each, and actor change/close clears both. Retired65 is retained authority even when newer77 is loaded; neither latest projection nor current workerNET is substituted. A copied or modified transfer object refuses.
* Each transfer rendering adds one `transferNetDescription`; its generic record contains no repeated NET scalar. Cache absence names the exact projection and prompts the existing Projections link, never “NET not entered”. Returned/reissue cash legs retain original transfer/return lineage and do not manufacture a new payroll input. The existing complete frozen child graph, pagination, money strings, escaping, modal/focus lifecycle and actor invalidation remain intact.
* Current0623 includes the PS-07 controlling0614 closure, which includes original1206 unchanged. Registry designates one final owner with the original physicaldefiner; laterEX41/CM18 preserve that merged authority. No schema, financial producer, public signature, grants, route, Summary/filter/export implementation or Rule-B owner changed.
* FE asset version is updated to `20261010-next-recorded-net-r1`; Bible append describes local semantics and explicitly leaves connected acceptance NOT_RUN. BD037/D04 similarly distinguish source behavior from later joined proof.

## Existing case/source coverage

Reviewed added ordinary cases without execution. FE checks currentPAYE NULL, Umbrella and CASE_PAYOUT NULL, exactzero, all four unallocated line contexts, injected lineNET refusal, one65 description, explicit missing-cache copy and foreign projection rejection. Its retained-record path case loads old65 plus new77, requires exact65, refuses modified-object substitution and proves actor invalidation through the actual client path. BE covers money shape/negative/numeric/null contradictions, the actual request kind, all five prohibited allocationfields and finite no-split negatives. Existing closed52 allowlist, actor/foreignscope/cursor/transport/error and ordinary UI behavior coverage remains present. These are source/pure controls, not financial journey fixtures or proof that a supplied65 was actually paid.

D04 accurately remains NOT_RUN for the actual twoTimesheet65, zero-versusNULL and100less30basis journeys. FutureAUTH-PS/S1 filter/CSV/print owners must retain these semantics; no such report capability is claimed by this change.

### Fresh source closure: original request scale

Root's independent server review identified that0380's exact numeric domain does not impose fixed output scale: a valid original NET request can retain `65`, `65.0` or `0`. The first reviewed strict two-decimal regex would reject those genuine rows. Re-read the corrected saved helpers and added existing case operands: both mirrors now accept the finite nonnegative integer/one-or-two-decimal money form and return its **original string**, without formatting, coercion or zero default. CASE_PAYOUT/null, negative/numeric/wrong-kind refusal and unallocated line rules remain unchanged. The BE case explicitly covers actual NET_REQUEST for all three scales; FE display coverage preserves each original scale. **Scale issue closed; fresh source PASS.** Root reports10 BE/13 FE cases and syntax passed; reviewer did not execute them.

## Raw reviewed hashes

| File | SHA-256 |
| --- | --- |
|FE `js/bpay-next-payment-history.js` |`7fff5c29876941649df8a984a4c2187b69ed1d952c2ca77fc8825833018e1b24` |
|FE existing history tests |`308cfe92176792c45f795e62e1715ceae8880874581547625daf3ec1f87a89c2` |
|FE `index.html` |`ba5685e78ae479728db131932b5d803cdf59478168816abd75758d9777f09669` |
|FE `BANKING_PAY_BIBLE.md` |`3fd1f72d6db303f20fb43a98df716ccea973a75ac3fb7d8ac874a3e2dbc1795f` |
|BE `broker/src/bpay-next-history.js` |`a402fa5f72baf341235b0c35a513bed6cb47c5df17e3b7f461918e86a7a29cb9` |
|BE existing history caller tests |`b0c1f69431e2a5753c866da283e67669b116f0c43df888c70a31d13fb1cbe17b` |
|0623 scoped history closure |`f53ca9dae5e297c628b5e1abcf491ceec1dd3bbea05e99aa2fea61fec108573e` |
|0380 genuine Case NET schema |`9b750149eb86e7ddd6cff218783cbff2674c64abf3a117f20e66579d5f3d291e` |
|BD037 scope |`708f80a0c61aa8e0f4c47b883402a6045863ccc2b407c6af146e1c096bc401e9` |
|D04 future joined note |`636ed0323587deca96300b1d65c39ae8dd863b6680b3de0d9bb93047627d2ece` |
