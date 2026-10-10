# PS-15 caller / compatibility review

10 October 2026. Independent saved-source review; **PASS for the current D02/D03/D04 compatibility slice. No open concrete defect found.** This is not installed, timing, whole-Summary, export or joined acceptance evidence. No SQL, tests, runtime, business data, hosted reads, fixtures or network operations were executed by this reviewer.

Read the full PS-15 card06:1636–1660, all seven exact checklist rows, controlling R2§7, current BD039 scope, PSC_CLOSURE and the NOT_RUN joined checklist. The owner's recorded AUTH-PS approval supersedes the older card/checklist/investigation wording about unapproved Summary UI. Approved S1 sequence287 owns its later list/filter/layout/consistent-export implementation; no additional approval or competing API is required here.

## Current source closure

- The final registry selects one `public.bpay_next_payment_history_page_v1(jsonb)` root and physical definer, new0653, after1206→0614→0623. Its body outside the `$history$` metadata literal is equal to original1206 after newline normalization, including service-role/active-admin checks, exact ancestry, cursor rules, byte limits, stable/security-definer configuration, logical owner, browser-role revocations and service EXECUTE grant.
- Parsing the two source metadata literals found exactly one changed section: REVISIONS adds `candidate_display_name`, `candidate_reference`, `client_display_name`, `job_title`, `band_label`, `timesheet_reference`. These are actual captured `private.bpay_next_work_revision` columns in core1324:154–159. All other section metadata is unchanged. Independent source-text comparison found all52 SQL section parent/key/field/amount-scope definitions equal to both Worker and Office decoder schemas. These additions use the existing scalar STRING_OR_NULL projection; there is no live Client/Timesheet lookup, duplicate capture, pricing or line NET allocation.
- Worker request validation and fixed RPC transport are otherwise unchanged. Actual index.js:198707–198739 authenticates admin, applies the existing TEST environment guard and invokes the fixed service RPC with the server actor. The exact history POST path remains exempt from the retired legacy fence at bpay-next-legacy-fence.js:172. Each read is one bounded page; the closed request/cursor contract,12KiB request,128KiB response and8s abort remain. This does not prove database cancellation at8s.
- The history reader's actual immutable revision/line/shift ancestry and SHIFTS/BREAKS/RATES/SCHEDULE pages have no READY, active-owner or current-revision selection predicate. Sealed revision ancestry remains required. The separate operational0250 Draft-review frozen-detail guard is preserved, as the scope and joined checklist explicitly state.
- Office changed only the same six REVISIONS field names. Its exact schema/actor/cursor checks and existing escaped scalar renderer remain; the six captured strings are escaped at payment-history.js:473. The history asset version and the existing corresponding asset assertion now select `20261010-next-history-attribution-r1`. No new Summary layout, export or filter API was added.

## Finite existing source controls

Read the appended PS15 case in `tests/bpay-next-payment-history-caller.test.js:51` and the preserved neighboring cases. It selects the registry's current physical definer, checks absence of the retired owner/READY filter, positively identifies all four immutable frozen-child sections and captured Client attribution in REVISIONS, and sends actual closed-handler requests for CSV_EXPORT, PRINT_EXPORT, a Client filter, export_format and caller totals. Every request must return400 and the shared RPC counter must stay zero. Unknown sections and unknown fields therefore cannot become a partial report. Existing all52 metadata parity, auth/service actor, scope/cursor, byte/UTF8/privacy and NULL/exact-decimal controls remain intact. Existing FE parity independently compares the mirrored52 definitions; its only case edit is the real asset version assertion. No new harness or data scenario was introduced. The source controls were read, not run by this reviewer.

PSC_CLOSURE correctly separates current indexed/scoped live-keyset compatibility from the later consistent filtered export, distinguishes Source FORCE-RLS from owner-only NEXT table ACLs, records unavailable moved-root receipts honestly and reports no newly evidenced missing exact index. The separate index investigation remains source support rather than query-plan/latency proof. PS-15-JOINED-ACCEPTANCE keeps every real in-app clause NOT_RUN, including prepared/paid/cancelled/returned traversal, foreign scope and export refusal, late commits and later S1 obligations.

## Reviewed raw SHA256

| Saved source | SHA256 |
| --- | --- |
| supabase/repeatable/10102026_0653_banking_pay_next_ps_c16_indexed_history_v1.sql | `a6c76d789c3ffae22ed7b7749fa0943807a16c0477cf8cbe2cd56e878c33cec3` |
| broker/src/bpay-next-history.js | `1262a7961045a44b294939257db93e2ef54a3cf0754f76f90e8ebc52982335e0` |
| tests/bpay-next-payment-history-caller.test.js | `4fbc1dab6100e02f6608f1eea913649e200859b14093cb959265923a94653747` |
| BUILD-FE/js/bpay-next-payment-history.js | `467a78cd820f61cb0ee0e461f02f78d6c06f468024e7121f629770351f074d4e` |
| BUILD-FE/tests/unit/bpay-next-payment-history.test.cjs | `9530138f06c9ce5ad0ab62a09e945041f730b24b1b0d1a811bced1549a38934a` |
| BUILD-FE/index.html | `700b823b40a29ba947fcdf977a25eb54c308da7a69f76fe7b4c0bdd07fd98592` |
| docs/bpay-next/design/PS-15-SCOPE.md | `4c2ab58001b6589dfd43058530684a75b683f45af954acd57f2d60ad4ca8951b` |
| docs/bpay-next/PSC_CLOSURE.md | `f68da4e70a5a78b74df82d58774c39df928b0ee57bfe74bc1eb9b4fb3399cc1e` |
| docs/bpay-next/PS-15-JOINED-ACCEPTANCE.md | `c5eedba5b9ec3b880111e4faebbe4e7774a0140dea7319bf711cc6bae882c7a2` |
| docs/bpay-next/final-definer-closures.json | `ac856b9f9bd3866f2e080c1272b4fe4922d093708b9a980ad0ce8ec4a7d870e3` |

Final verdict: current compatibility/caller source PASS; no claim that S1/export, installed behavior or future joined checks have passed.
