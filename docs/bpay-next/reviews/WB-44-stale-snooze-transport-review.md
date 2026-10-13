# WB-44 stale-snooze transport/native phase review

12 October 2026. Independent read-only source review, Sol6.1 Extra High. Scope: the three current transport validator diffs and the current financial service/Retry owner additions in `12102026_0016_banking_pay_next_stale_snooze_publication_v1.sql`. Design authority is amended BD128/WB44 owner A2 and `WB-44-stale-snooze-design-review.md`. No product edits, executions, database/business calls, tests, fixtures, screenshots, hosted actions or commits.

**SCOPED SOURCE PASS on the final saved bytes below.** W44-T01 is corrected and freshly reread. This receipt does not review every apply/cancellation guard body or replace the separate SQL review.

## W44-T01 — canonical lowercase SNOOZE cursors — CLOSED

`broker/src/bpay-next-job-step.js:199,230,267` adds the right conditional UUID checks, but calls the existing generic `field(value,'uuid?')`. That generic UUID regex at42 has the case-insensitive flag. The three new checks therefore accept uppercase SNOOZE cursor text, while the native service0016:848/920–922 and both Worker/FE Retry validators require canonical lowercase UUID text.

Root made the smallest correction: dedicated `snoozeCursor` at126 requires a string, the existing complete UUID pattern and `value === value.toLowerCase()`; the original optional-field handling permits NULL. Request200, CLAIM231 and STEP269 use that field only for SNOOZE. Fresh full diff confirms ordinary UUID acceptance and NEW/REMOVED component cursor handling are unchanged. No new field, schema version or helper protocol was introduced. The original mismatch was reported before this correction; it is no longer outstanding.

## Scoped source conclusions

The actual SQL service diff against its complete current2259 parent adds SNOOZE only to the POSITION STEP/CLAIM phase admission and conditional UUID cursor checks. Request keys, service/NEXT authority, 2048-byte envelope, lease bounds, original claim owner, predecessor exclusion, original result validation and own successful READY yield are retained. PREPARE/PF08, expiry/discard, Case and other financial phase arrays are unchanged. New SNOOZE does not expose or adopt a stored active nonce.

The actual Retry helper diff against current2259 adds SNOOZE to the complete-financial-line count condition and pending publication phases. It additionally requires APPLYING/SEALED and a NULL or canonical UUID cursor belonging to the exact Work's retained snooze row. It retains predecessor Work.applied, original request/actor exact replay before current-state checks, original typed job/module/owner/cursor comparison, definite FAILED requirement and repair solely from the native publication checkpoint. It does not recalculate money, change original accepted inputs or enter PREPARE retry semantics.

The Worker/FE Workbench receipt diffs match: SNOOZE is admitted only on received non-PREPARE READY results; non-NULL cursor must match lowercase UUID text. Exact receipt keys, requested identities, epoch equality, unrepaired phase/cursor equality, DONE/null cursor, retained unknown/readback behavior and PREPARE qualification remain intact. A repaired result can truthfully return the publication checkpoint without guessing the submitted cursor.

Job-step keeps the original closed kind and argument/output keys, request2048/reply128KiB/deadline8s/page1–100 bounds. It adds no POSITION replay boolean, and still permits genuine zero-row phase completion. Its STEP result retains done↔DONE, terminal NULL cursor, NULL yielded lease, current owner epoch for nonterminal pages, and rows_visited≤requested limit. SNOOZE is not added to generic claimPhase or PREPARE fields.

Existing `broker/src/bpay-next-financial-queue.js:116–123` copies the native POSITION phase/cursor and stops an unchanged same-phase checkpoint; no queue change is required. Actual ID-only wake, original retained STEP input and fresh CLAIM ownership continue unchanged. The new migration is only the native publication phase CHECK extension and active exact-Work UUID seek index; there is no new command/kind/route/table or visible UI flow.

## Evidence boundary

Current implementation inspection is source-only. SQL guard/application behavior, compilation/catalogue, runtime transport, unknown-response recovery, more-than100 controls, user journeys and timing remain **NOT_RUN** by this reviewer. Root's independently reported normal checks are not this reviewer's executions. No essential scoped source finding remains. Visible UI behavior was not changed by these three validators, so this review does not create a screenshot task.

## Final reviewed raw identities

| Artifact | SHA256 |
| --- | --- |
| `supabase/repeatable/12102026_0016_banking_pay_next_stale_snooze_publication_v1.sql` | `2d9822da9881179dace5a2ff97c65fcaaf1dfbaad920337a4701133d8fe487ae` |
| `supabase/migrations/12102026_0016_banking_pay_next_stale_snooze_continuation.sql` | `d837eccb1f0a98a4fff446c98461f46f250b078bb63b2f85fd91882ba9f2fc42` |
| BE `broker/src/bpay-next-job-step.js` | `01374c96e62f6e78242f53b75a4ef13b8a624024a1763f5b10b063d1a71c8faa` |
| BE `broker/src/bpay-next-workbench.js` | `c65bcc36dbc4b2283b0c3e4a6fc4041c209de3feda77c4fa9ceb59236d09a514` |
| FE `js/bpay-next-workbench.js` | `f33e33cc748b3f155b45eac1649cbf82cd568c69e9aeb021175c32d483a2cc04` |
| `docs/bpay-next/final-definer-closures.json` | `76f9a407c3268aaa3ba1ec8339c8e898dff584b473e27535cc9b6fe2175acc07` |
