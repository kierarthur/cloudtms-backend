# WB-23 transport source review

10 October 2026. Independent source-only review of BE `broker/src/bpay-next-preparation.js` and FE `js/bpay-next-office.js`, their complete current diffs and relevant unchanged bounds/error/identity machinery. Checked actual new SQL `10102026_2232_banking_pay_next_selection_pages_v1.sql` constructors and retained reader at119–139/589–592 as contract evidence, alongside the full WB23 card/checklist and BD076. No product edits, module execution, SQL, database, tests, source gates, fixtures, browser or hosted calls performed. The root's reported nine source-gate passes are not an independent runtime result.

Initial verdict: two finite decoder findings required closure. Both are closed by the fresh-source supplement below. The uploader is a separate review boundary; this review does not approve its phase dispatch, whole WB23 or later Case/progress joins.

## Findings

**WT01 — new Office receipts must bind page_no.** At FE `receipt()`277–284 the common correlated identity list omits page_no. EXCLUSION_PAGE validates item_count and MATCHING_PAGE validates item_count/complete/cursor, but neither compares returned page_no with the request. A well-typed receipt for another page reaches the caller as accepted. Old WORK_PAGE/COMPONENT_PAGE/CASE_PAGE add their own exact page checks; BE result() does bind page_no. Add the same finite new-action comparison before journal adoption. Source evidence: a request page2 and otherwise valid response page1 satisfy the current FE checks; no execution was used to manufacture that response.

**WT02 — Worker retained-input duplicate native UUID check must normalize case.** At BE accepted_inputs row loop351, UUID field uses the existing case-insensitive UUID pattern, but ids.has(row.work_id)/ids.add(row.work_id) stores case-sensitive strings. Same UUID in two successive member ordinals with differing case passes that uniqueness check. FE counterpart432 correctly uses lower-case Set keys. Use lower-case identity keys in BE too. This concerns malformed reply refusal; actual SQL emits canonical UUIDs and the FE currently rejects such duplicates.

## Source boundaries confirmed

- New EXCLUSION_PAGE exact args are run/page/two UUID arrays. Dedicated emptyIds permits0–100 individually, exact command validation limits their **combined** size<=100 and rejects duplicate IDs case-insensitively within each native kind. No weakening of original nonempty ids/WORK_PAGE choices, no financial arguments.
- New MATCHING_PAGE exact args are run/page/positive-or-null after_member_no. Integer strings retain signed bigint ceiling; no JavaScript money conversion. Receipt accepts integer item_count but explicitly caps it100, bounds unique skipped entries by attempts, requires complete iff next cursor is null, and requires nonterminal cursor=original cursor+attempted count with at least one attempt. Complete zero page is supported; ordinary WORK_PAGE retains positive item_count.
- SQL2232 exact exclusion receipt is run_id/page_no/item_count/complete/replay. Matching is the old bounded ACK receipt plus complete/next_after_member_no, with atomic immutable captured request/result. Both transport maps match these keys. They add no agency command or provider semantics.
- accepted_inputs is an **optional discriminator**, but when supplied accepts only literal true. BE URL parser converts exactly the string `true` to boolean; `false`, alternate spelling, repeats, missing/extra cursor keys or after_work_id on the true branch refuse. Original absent discriminator keeps the normal UUID-cursor schema. FE supplies boolean true and serializes it consistently. No new read kind/route/RPC transport.
- Retained header exactly matches SQL real native run/uploaded_choice_work_count/selection_page_count/selection_count/exclusion_page_count, with integer strings. Rows match member_no/Candidate/Work/captured revision/ALL-or-SUBSET/nullable existing skipCode. Each ordinal must equal previous+1, pages<=requested<=100, nonterminal pages cannot be empty. FE verifies exact next_cursor. This supplies the previously missing raw upload-prefix evidence without current eligibility or monetary rows.
- Ordinary six-key WORK_CHOICES, its UUID cursor and ALL/SEALED/zero-child invariants remain intact. Existing post-seal readback atOffice568+ remains the ordinary variant. Parked ACCEPTED_* calls are not introduced by this diff.
- Shared raw transport derives action admission from the existing maps and retains fixed intake/page RPC endpoints, caller actor header, abort signal, request/response byte limits and deadline. No generic RPC, extra route, browser-role grant, automatic business retry or accepted-journal clearing appears in this transport slice.
- New Office Ready join passes actual root readWindow.active() to the existing local UI; no product money owner/RuleB/producer changes appear. Its future control/sequencer behavior is a separate review boundary.

Checkpoint raw SHA256: BE preparation `9b578413cee6a0cd801f137167fa3dbc83a1ab47c661153b20a2fa61410827c6`; FE Office `6a051ae19c91b521ec2221b26c5a7a5784d305c75a5434e5b2dbaa0d3f7a8ca0`. Root concurrent sequencer edits may subsequently change FE hash; this checkpoint binds only the source reviewed here.

## Fresh transport closure

Read current saved source after root fixes. **WT01 CLOSED**: Office receipt281 now compares page_no for both new actions. **WT02 CLOSED**: Worker retained row351 uses lower-case Work UUID Set keys on both lookup and insertion. Their complete current diffs preserve the ordinary input/row/cursor/receipt contracts and original bounds; no remaining concrete transport defect found. **SOURCE PASS for this transport slice**, with no execution or whole-uploader/UI approval.

Closure hashes: BE preparation `0e82517412bcf9892a238abaa5691c231cca24d4e149fb0cd099b0a2310f5050`; FE Office `ce82f953cd5c630193993c21f163e2bd84096943f3bc51289a17cf341000618b`. The FE hash includes the simultaneously authored dispatcher, which this transport closure does not approve; its independent review is `WB-23-sequencer-review.md`.

## Native subset context supplement

Re-read fresh2245 private reader and complete BE/FE retained-input decoder additions. The three new nullable original-choice fields match actual native selection_state/page_count/component_count, with exact run/work point join and run FOR SHARE. Both decoders pin their joint nullability to the original captured skip_code; unskipped ALL is SEALED/zero pages/zero components. UUID duplicate normalization and new action page correlation remain present; old ordinary six-field reader remains unchanged. **Transport SOURCE PASS retained**, with no extra route/kind/public signature/grant or opaque financial fields.

Updated checkpoint hashes: BE preparation `249ec176f83f512d5ea1b521bcc8bdb6c49c0a872f3c37b1c8650115f22dc5a8`; FE Office `e0e190c969d2bfc3dadafa896ae40b75acb4452838ce271df7a6b2dfd1320340`; private2245 `e1c9936055a24b271b887ee431a8f8dd4e2a224d1b8bb184f3e709534e1fabb4`. The separate sequencer supplement records its own finite source approval; later actual UI/whole-item checks remain outside this review.
