# WB-12 Ready transport and UI source review — 10 October 2026

**SOURCE PASS after RUI01 correction. No remaining concrete defect found in the reviewed slice.** Read-only independent review of current saved source against the full WB-12 card, eight checklist rows, R2 replacement, current scope/design notes and the owner’s later screen rules. No tests, source gates, imports, SQL, database, browser, business/data, hosted or network operations were performed. Product fixes were made by the root builder only. This is not rendered-screen, application or installation verification.

## Reviewed source and hashes

Paths below are relative to the stated BUILD worktree; SHA-256 values cover the saved raw bytes.

| Worktree / file | SHA-256 |
|---|---|
| BUILD-BE `supabase/migrations/10102026_1743_banking_pay_next_ready_candidate_read_indexes_v1.sql` | `3b471ee4cbe26087f2c8a02040aa190a536288627d213de1e874b42f3ae834fc` |
| BUILD-BE `supabase/repeatable/10102026_1743_banking_pay_next_ready_candidate_counts_v1.sql` | `727d80356ecfd7ef7e8c822c3181403602910ab4cef5bb50f4677dd32df2b760` |
| BUILD-BE `broker/src/bpay-next-workbench.js` | `19ba3f30fd416604b22bf92f6e5e5b5b7c966bf7b100e67d0941cfa6512a6910` |
| BUILD-FE `js/bpay-next-workbench.js` | `4cdf12a964f25b4c8ebd2d6ca05d03c7387cd36bf8d0c45c3857a81f570adbd8` |
| BUILD-FE `js/bpay-next-office.js` | `d377ce9c8eb7073169f045d997200c19c47a828bcf88ff5fcdf9b9df9696c189` |
| BUILD-FE `css/bpay-next-office.css` | `158a6e2f556e1bb8d8bc3c57e4267a5dae15602be8a9d8b6294ab69f68f357e4` |
| BUILD-FE `css/modal-modernisation.css` | `a4003bf69088f282c7883ab394a5b679826e8937707da0593456a5045b8e3606` |
| BUILD-FE `js/modal-modernisation.js` | `98ea1f99c329868d773ab99669349efab150b9077ebdb1f94dd5a5097589bcdb` |
| BUILD-FE `js/bpay-next-ui.js` (reused unchanged frame adapter) | `dbdcb12ed8ec027b0df39c52ef6f50b6d91ca93e2cb74451a59e09a6ba029272` |
| BUILD-FE `index.html` | `6fe220c0084a7adb37204fb62075497c17dfe89422ce572940d4de2bd8501b19` |

## Transport, counts and cursor agreement

The 1743 source is a full successor of the current 1634 common reader and public wrapper. Static whole-body comparison found only the intended Ready count projection, optional count-intent validation/default, Status keyset additions and their declarations. Existing issue provenance, fixed query templates, deterministic parent/group paging, current/adoption barrier, service/admin authority, STABLE/search-path/5s configuration, closed refusals and ACL closure remain. The registry selects exactly those two final routines at 1743. The two added indexes match the natural summary scope and the implemented Status tuple.

`count_selection` has exactly `mode: ALL|NONE` and unique lowercase native `candidate_ids`. Non-null intent is accepted only for HEADER/READY. Its null default remains in the public scope/context for every kind; both callers intentionally mirror that exact contract. The Worker parses the JSON field with its duplicate-key rejecting parser and retains the whole-query/request 32KiB bound. Response fields remain strict allowlists with decimal/integer text and the existing 128KiB transport envelope.

Ready Candidate count uses the authoritative `(client_scope,pay_channel,section)` summary universe with `payable_count>0`. Deductions-only Candidates stay in the Candidate rows with disabled ticks and “Nothing to pay this week”; they are excluded from eligible/selected Candidate counts. ALL exceptions and NONE inclusions use the same native UUID membership predicate. The new member count is the complete eligible Candidate universe’s payable-plus-deduction denominator, rather than a sum of loaded rows. Old `selected_count/selected_amount` retains its native accepted-reservation meaning; the UI does not relabel it as local selection. Missing count intent returns nullable selected counts, while a genuine empty selection returns text `0`.

SQL tuple/seek/key/order, index, Worker and FE agree on Status `(payable_count=0, bank_warning_code IS NULL, coalesced warning COLLATE C, candidate_sort_name COLLATE C, candidate_id)`, including descending order on every key and native UUID final tie-break. Existing Name/Amount/Deductions order remains. Both mirrors bind the full scope, actor, agency, epoch, date, workbench/adoption revision, receipt and continuation; no visible-page sorting was added. Header count results are accepted only for the captured intent revision and common workbench context.

## Ready component and actual Office join

The new mount reads the existing CANDIDATES and HEADER kinds and checks their common revision before adopting either. It retains at most three 100-row blocks plus compact bounded cursor-key history, preserves sort/scope continuation, and refuses stale results. Local choices store IDs only. Scope changes reset the ALL intent; sort/Refresh retain the choice intent. Rendered amounts use retained decimal text with presentation-only rounding; no browser sum, valuation, cash split, earnings owner, publisher, Rule-B, PIN or producer change was added.

Office mounts the component in the real WB11 Ready host, shares the existing client, joins shell availability/context, forwards scope/Refresh and closes it with the actual window. Actor/owner and generation checks fence late responses; unavailable or changed joined context hides the table and disables selection/opening. Single-click highlight, keyboard navigation, double-click/Enter and phone Open are present. Genuine bank warnings remain informational and open the actual Candidate record. The existing Candidate-record fallback is explicitly retained until the named WB15 child join; WB21 Create, WB22 persisted choice namespace, WB24 child overrides and WB19 progress remain later owners, with no manufactured early completion.

The Ready table reuses the universal card-table class. The 13 existing phone-card selectors now include only the actual Banking read kind, retaining their declarations and desktop scope. Phone sorting/Open and bank links have 44px support. Header labels strip the sort arrow only inside this Ready list. Row highlight focus uses `preventScroll`, and existing shared focus continuity is reused. All five changed style/script assets have updated cache identities in `index.html`; the unchanged shared adapter keeps its prior version. No runtime layout result is inferred here.

## RUI01 — closed: stale retained empty result looked like a current empty state

Initial `js/bpay-next-workbench.js:713` showed “No payments are ready” from a retained empty Candidate page/header even after `joinHeader` reported unavailable or a changed revision. During a failed or pending shell refresh the table was correctly hidden, but that independent empty paragraph could remain beside the stale warning. Reported to the builder before this verdict.

Fresh source now uses `if(available && !pages.some(...))`. Here `available` is the full `coherent()` predicate, including current actor/owner, successful local reads and the joined current header revision. Consequently an unavailable/failed/stale read cannot present a true-empty result. The final FE hash above includes this correction.

## Acceptance boundary

WB-12 D06 and its necessary typed reader/caller support pass this source review. Exact Candidate-membership behavior, real pagination at the specified limits, phone/desktop visual acceptance and later joined owners still require their named acceptance; this review records no application first use or hosted result. Parent-reported images/checks are separate evidence and were not executed or independently replayed by this reviewer.
