# WB-14 issue transport and current UI source review

Independent read-only review, 10 October 2026. Verdict: **SOURCE PASS for the current reason-sort reader, strict transport mirrors, five-column lists/detail, and genuine Timesheet window join. No actionable product defect found in this reviewed slice. This is not whole WB-14 completion or application acceptance.**

Read the full WB-14 card (`02_WORKBENCH.md:591–620`), all three current checklist rows, the current screen scope, earlier action-owner investigation, both 1806 SQL definitions/indexes, their entire diff against the two 1743 predecessors, current Worker mirror, Office/main joins, issue-view implementation, and changed CSS/modal labels/cache references. No product edits, module imports, tests, gates, SQL, runtime, browser, data, hosted, network, fixture, or secret operations were performed. Parent alone implements and runs authorised checks.

## Reviewed source identities

| Object | SHA-256 |
|---|---|
| BE migration `10102026_1806_banking_pay_next_issue_reason_read_indexes_v1.sql` | `99e877d811dd5b530c980930fc9954f807a241ea831f65d28a1bdb1c60c16b65` |
| BE repeatable `10102026_1806_banking_pay_next_issue_reason_reader_v1.sql` | `bbde6d21a6aad4eeb5bfa0741976246f58fbdebd6410c163d759fcf3321c8329` |
| BE `broker/src/bpay-next-workbench.js` | `bb8062751b52dd956bcb8cae01cb55a0ede80d255512388f4d41ff6470672824` |
| FE `js/bpay-next-workbench.js` | `bc1202e566769c5770335dcbb50046ceb3732e67a674ad69c3ddb77d44c8f958` |
| FE `js/bpay-next-office.js` | `73239f720c497fd17daea4e09cfbe2fead8a0a0443d0067796c2ad7e8a1344f1` |
| FE `js/main.js` | `cd8cd5077d61d5d48182904fbd7c5b4b35efa4df1177994101d6c63f0cbb583f` |
| FE `css/bpay-next-office.css` | `db86e164196cebdf3569d442858bdc041bb1c1a710d772c62ba149a01349097d` |
| FE `js/modal-modernisation.js` | `2b22a4793f66b11ca877c138fb0bb9c7378d135d3894a108cfd13b0dad3e8e38` |
| FE `index.html` | `b169ac333e24ab05cb77857ab702a17c2228bc7b74e9d50e67dc2fb642816b34` |
| BE `design/WB-14-screen-scope.md` | `baaf61f951cf9ee26f8eaa17925bcd268c3828420b6840155089ad32d6da0a14` |

## Source evidence

1. The entire 1806-versus-1743 SQL diff changes only the declared affected-count type, exact scoped distinct-Candidate count, REASON order/seek/key and corresponding finite validation. HEADER/count selection, Status ordering, amount fields, fixed SQL templates, current/adoption barrier, actor/scope/cursor membership, page limits, response envelope, original exception adapter, function configuration, ownership, service-only public ACL, notification and transaction boundaries remain preserved. Registry final winners for the common reader and public wrapper are 1806; no other Banking NEXT reader is redefined.

2. REASON uses `(reason_priority, reason_code COLLATE "C", native item UUID)` for both seek and scalar order (`1806:416–419,439`), with direction applied consistently. The section-leading and exact-group-leading indexes use actual native columns. Native priority is constrained to 1–200 (`1252` migration:106), and Worker/FE use the existing finite integer `small` validator for row and cursor operands. The SQL cursor parser additionally checks numeric/integer syntax before the smallint seek; non-native cursor keys still have to match a real scoped row. No money is converted to a floating-point number. Affected Candidate count is computed before the cursor filter (`1806:445–447`), so scrolling cannot turn a whole-scope count into a page count.

3. Worker and FE retain exact envelopes/row schemas, actor and full request context, cursor receipt identity, duplicate-row/order checks, limit and byte guards. Only the three issue kinds accept REASON sorts. Decimal money remains text; numeric comparison is confined to the bounded integer priority. The wider integer count decoder does not fabricate a multi-Candidate cause: current native group selection still determines the real returned count.

4. The current view (`FE workbench:739–799`) renders exactly five columns, uses server sorts, and keeps at most three 100-row blocks plus a byte-bounded cursor history. Search/scope/kind resets discard inapplicable old membership; sort/Refresh may retain old rows grey while reading. Generation tickets discard stale reads. Failed reads preserve earlier valid figures, disable navigation, and retry the retained direction/cursor; typed stale refusals require a fresh read. Parent availability plus shared revision identity prevents opening a detail from an incoherent main view. No browser total or selection money is calculated.

5. Main rows highlight on click and open on double-click/Enter or the phone Open control. Child controls stop propagation. `invoke` checks actor/NEXT/current context and the actual frame's `canInteract` before callbacks (`:750–755`). The Office root supplies `readWindow.active()`, and detail supplies its own `frame.active()` (`Office:570`, workbench:819). The real Timesheet callback rechecks actor/NEXT and passes the retained native ID as `openTimesheet({timesheet_id: ...})` (`main:7165–7170`). This closes the earlier incorrect `{id}` shorthand and one-argument issue-window callback seams.

6. Office creates its own two-argument `openIssueWindow(options,address)` and retains the parent (`Office:571`). Detail reads the exact retained native group/reason/section and original scope, with the initial parent revision enforced. Explicit detail Refresh permits adopting a new valid revision and then invalidates the old main issue/header views (`workbench:817–819`, Office:571). Refresh is active-frame gated. Dismiss closes the view, invalidates late reads, and restores focus only for the same actor, current owner and connected former control. No callback issues a financial POST or automatic retry.

7. Generic case/snooze/hold/restore labels have been removed from current `issueActions`; only a supplied actual Timesheet callback is wired by main. The optional query callback has no current attachment. No reason code is treated as proof of a finance-resolution family. Phone controls have explicit 44px minimum height; the existing universal card table and theme variables are reused, Candidate label suppression has the necessary scoped override, and the modern label helper strips only sort-arrow suffixes for these actual lists. Layout/application acceptance remains the parent's separate obligation.

## Remaining scope and documentation precision

The specific three-Candidate shared BLOCKED-cause journey is explicitly unresolved in the root scope; the current native group has no durable shared cause connecting unrelated Candidates. Existing private Source OPEN-display information is not yet attached here. Later rate/linked/clear/snooze/exclusion/cancel/restore/breakdown controls must use their actual typed owning interfaces when built. These are not discharged by this source PASS, and no fake multi-Candidate group, guessed family, early dispatcher, or legacy financial POST is endorsed.

One paper correction is advisable before a completion note: `WB-14-screen-scope.md:58` calls gross resolution WB-32. The controlling plan places non-bucket gross total in **WB-31** (`02_WORKBENCH.md:1084,1092,1168`); WB-32 owns pay-channel decisions/taxable-channel restructure (`:1193`). This does not change the current product slice because no such callback is attached.

Reviewer runtime and joined acceptance: **NOT_RUN**. Whole-card completion: **not asserted**.

## Final saved-source refresh

Re-read the final changes after the initial review. SOURCE PASS remains for the same bounded slice. The inline typed-stale Refresh now calls the same active-frame `refreshDetail` function as the footer, so it releases only the opening revision constraint and does not retry the stale original context indefinitely. Earlier valid affected counts/footer copy remain visible during a retained refresh; singular `payment`/`Candidate affected` wording is exact. The CSS now uses the actual `--ctms-modal-*` theme tokens for muted copy, selection, focus and alert states. The main Timesheet callback is unchanged, and its main asset marker is now `bpay-next-standard-window=20261010-r2`; related current issue assets remain explicitly versioned. The scope now attributes gross choices to WB-31, closing the paper precision above.

Final changed hashes:

| Object | SHA-256 |
|---|---|
| FE `js/bpay-next-workbench.js` | `906c966bc4a671b1cc72a822071367172a1c1aff33764b6f9981bb25db845f3b` |
| FE `css/bpay-next-office.css` | `e80c90f378ca725671040f554524ca406861804c0414c049a53f9ea81d8d4ec6` |
| FE `index.html` | `fc9d636114faacf62fb6bc1f124ed95689ff9f8c7aa11f7551813737330a444a` |
| BE `design/WB-14-screen-scope.md` | `99ca0a4e8d6ef73a4e8eb9a5f541eb1411c2da36bec11b8d6405701027f0fefe` |

All other source hashes above remain unchanged. Empty-response fixture work is root-owned verification, not a reviewer-executed application result; the current code renders a true empty state only for coherent available reads. Runtime/joined acceptance remains NOT_RUN by this reviewer; unresolved scope above remains unresolved.
