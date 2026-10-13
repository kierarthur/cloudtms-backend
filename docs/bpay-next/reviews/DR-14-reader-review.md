# DR-14 waiting-owner and captured-metadata source review

12 October 2026. Independent read-only review of the uninstalled 0404 slice. **SOURCE PASS for this slice; no essential defect found.** This is not an ITEM, application, timing or joined-acceptance PASS. The captured-metadata writer, public reader/Worker/Office additions and DR14-D05 blocked DR37 dependency are outside this slice and remain subject to their own review. No SQL, business RPC, test, fixture, hosted action or product edit was performed by this reviewer.

Authority: full DR14 card03:378–397, current checklist DR14 rows and `design/DR-14-build-scope.md`:3–9 (BD133). The existing native lease is an informational waiting fact; this helper neither claims a job nor changes allocation, ordering, retry, ownership or capacity.

## Actual current owners and binding

The registry resolves CLAIM and active-job validation to `supabase/repeatable/11102026_2259_banking_pay_next_prepare_retry_v1.sql`; run resolution to `11102026_0559_banking_pay_next_worker_reprepare_v1.sql`; OFFICE worker-action enrollment to `11102026_0814_banking_pay_next_preparation_discard_v1.sql`; general enrollment to `11102026_2041_banking_pay_next_case_pause_v1.sql`. These current bodies, rather than their older retained copies, controlled the comparison.

| Requirement | Exact source evidence and conclusion |
| --- | --- |
| Original Create before the first CLAIM | `12102026_0404_banking_pay_next_preparation_wait_owner_v1.sql`:14–32 qualifies the selected Candidate, its SEALED/PREPARING unconfirmed Run, current NEXT epoch and original Run-command-owned PREPARE job when no worker exists. It does not rely on `job.retry_run_id`, which need not yet be populated. Core1324:811–814 makes Run-command identity unique; :986–990 makes the command/Candidate/PREPARE job unique and binds it to actual command membership. |
| Existing worker and retained REPREPARE | 0404:25,43–45 uses the exact `active_prepare_job_id`, PREPARING worker status and current active-job validator. Current0814:2043–2054 inserts the OFFICE replacement job, records the immutable request binding, assigns this active job to the retained worker and seals the command in the same transaction. Therefore the earlier acceptance-time NULL active job in current2259:2806–2808 is not a missing waiting path once the command qualifies as SEALED. Current2259:367–379 retains economic/execution revision and exact unfinished request association; current0559:5–14 resolves original or OFFICE-reprepare Run identity. |
| Genuine live other owner | 0404:33–45 seeks the same Candidate's exact `worker_control.active_owner_epoch`, current module epoch, LEASED PREPARE job, non-null nonce and unexpired lease; the job and resolved Run must differ from the waiting job/Run and remain active under the existing validator. Expired, DONE, another-module, inactive-execution and same-Run jobs produce no waiting row. Core1324:991 makes LEASED nonce/deadline shape non-null. |
| Native claim correspondence | Current2259:1084–1099 requires current epoch, SEALED complete command enrollment, claimable job state and no attention/backoff; :1101–1106 preserves earlier-Candidate-command ordering; :1110–1138 admits the original missing-worker path and validates current execution; :1142–1153 increments the Candidate owner epoch and leases the native job. 0404 preserves that ownership relationship without implementing a second claim path. Its existence result reports the live other owner; it is not a claimability certificate and intentionally does not copy mutable lock-taking or claim side effects. |
| Actual starter name | 0404:38–41 resolves the owning Run through the native helper and reads that Run's `actor_user_id` from `public.tms_users`; :13 trims/bounds the real display name to 250 characters. Missing/blank names remain NULL. There is no invented actor, Candidate-directory traversal or inference from today's Work/Contract. |
| Isolation, nulls and ACL | 0404:11 is a read-only SQL function with pinned search path and SECURITY INVOKER; :47–48 preserves logical owner and revokes PUBLIC/browser/service execution. Null input IDs or absent native bindings return no row. Run/Candidate/module/job equality and exact active execution fence the fact; no money value is computed by this helper. Public-reader permission and read-only early dispatch still need review in their later slice. |

## Migration and bounded seeks

`supabase/migrations/12102026_0404_banking_pay_next_captured_other_draft_capacity.sql`:10–22 adds only nullable captured diagnostic source/target amounts and a finite cause on the existing instruction. Existing instructions retain the all-NULL unknown form. The CHECK explicitly requires both amounts together, non-negative amounts bounded by the instruction's existing nominal-minus-capacity operands, NULL cause for known zero, and one of `SAME_WEEK_INSTALMENT`/`CASE_BALANCE` for positive reduction. Half-null pairs and malformed causes cannot pass through CHECK UNKNOWN. Existing0300:252–259,286 makes nominal/capacity operands non-null and capacity no greater than nominal. Current0207 pure plan:313–322,364–375 supports PAYE/PAYE Case instructions with identical source/target ex-VAT operands, so the paired-positive shape preserves the currently supported valuation domain. Attribution must still be captured by the pending writer from actual locked foreign holds; this CHECK alone does not establish causation.

Migration0404:27–30 supplies a partial LEASED `(candidate_id,owner_epoch,module_epoch,id)` index for the exact current-owner point, with required native lease/command fields included. The original selected membership, worker, Run-command, unique command/Candidate/PREPARE and job/Run/user primary-key paths support the remaining joins; no additional ownership pointer is needed. :31–33 supplies the positive captured-diagnostic seek by exact worker/economic preparation revision. These are native index paths, not a universal physical-scan or timing claim.

No command kind, job kind, table, protocol, maintained counter, public trigger, money formula, allocation kernel, lease adoption or historical cause reconstruction is introduced in this slice. DR14-D05 remains `BLOCKED_ON_DR37` as BD133:5 explicitly records. Exact per-Case amounts use the future card-owned REVIEW_CASES reader; this waiting helper is not that renderer.

## Reviewed byte identities

| Input | SHA-256 |
| --- | --- |
| Migration0404 captured other-Draft capacity | `36569801CB8B8D8989150779E7E4D9AAF6590C169AA6E792ED86EE1D314EB225` |
| Repeatable0404 waiting-owner helper | `38F4DBFC0D50645CAAE36FE1B127270B5DE9F5DDEF7306E0DE51833C8F75E375` |
| Current2259 retry/CLAIM/active association | `1A57552C42D63D0DC885A4E7294472C03319537DA9E913A96CBBADC10C2AB879` |
| Current0814 OFFICE worker-action enrollment | `29F3099EBFB8A7E11D8C23A8996C5351EB9330316D8BDB6FD6D3A5136D3F379C` |
| Reviewed registry | `35B57B2E8EDBD3A1196281DD743F817A19753F313A2D71DF931D42D15728692C` |

Compile/catalogue/gates were not executed or observed by this reviewer for these uninstalled bytes. Joined behaviour, runtime timing and screen acceptance: **NOT_RUN**.

## 0414 reader/Worker/Office checkpoint — OPEN

Independent static review of the next uninstalled slice, 12 October. This checkpoint does not supersede the preceding 0404 SOURCE PASS. The SQL/closed transport additions are coherent; the waiting-list composition finding below prevents final Office source closure until corrected.

| Finding | Exact source and current disposition |
| --- | --- |
| W01 — polling/append lost the continuously browsed waiting window | Initial Office2098 reset all waiting pages on every PROGRESS poll; initial render2216 recreated the scroller without retaining its position. **Corrected on reread:** current Office1722–1731 supplies bounded visible-window refresh, :2101 uses it for polls, :2236 restores an actual visible row/offset and reset semantics. No new page store or all-page array was added. |
| W02 — phone removed the only waiting-sort controls | Initial CSS973 hid the thead containing both server sort buttons. **Corrected on reread:** current CSS973 keeps those headings available on phone; Candidate/State controls still request the existing server CANDIDATE/REASON sort keys. Actual pixels were not inspected by this reviewer. |
| W03 — refreshed visible blocks can overlap after live waiting membership changes | **OPEN at checkpoint.** Office1723 refreshes every old block using `block.after`. The live helper excludes an expired other lease without changing the waiting Run's revision/context. Given old pages 1–100 and 101–200, expiry of Candidate50 while tail100 stays eligible makes refreshed page1 end at101; the old second-page cursor after100 then returns101 again. Each individual response satisfies the existing closed contract, but the composed visible list contains a duplicate. Required narrow correction: keep the first visible block's original native cursor, then chain each freshly returned `next_cursor` for subsequent blocks, still at most three; stop on fresh completion. No new token, counter or proof framework is needed. |

Static parent comparison found only the intended waiting/captured-capacity additions in 0414's complete current0341 public-page body. Progress is the complete current2259 body plus its waiting EXISTS; the original PF08 attention predicate and stored run counters are unchanged. Public PREPARE_ISSUES still performs the existing service-role/admin permission and current NEXT/original epoch checks, exact original label joins, sort/tail validation, native context equality, row limits and byte ceilings (0414:120–160,163–216,238–303). `waiting_only` is a required closed Boolean, permitted true only for CANDIDATES, included in both initial/final context and cursor. Non-Candidate rows receive false waiting/capacity flags and NULL other-run/name fields through the Candidate-only lateral join (0414:223–227); capacity detection uses the immutable instruction's positive captured metadata at the worker's retained economic revision. It reads no current amount, headroom or inferred foreign reservation.

Worker `broker/src/bpay-next-preparation.js`:14–54 and FE Office205–245 carry matching input/context/row schemas. Both bind request/response waiting mode, require other Run identity exactly when waiting is true, prohibit an other actor label when waiting is false, reject the same Run and prohibit waiting/capacity metadata on omission rows. Worker537 parses only literal true/false. Existing PF08 attention, command journal, actor/namespace invalidation and mutation routes remain intact. Office's common capacity sentence is information from that captured Boolean, not a client estimate or a new deduction control. Per-Case exact amount rendering and D05 remain outside this slice as previously recorded.

Checkpoint input SHA-256: SQL0414 `8344BFF1A8ECD96E3FBB289F3B3F16CB37B56E5043B4B9D5999FBF2CE8D7FE9E`; Worker preparation `A57251A62FBBA523BA217F262C936F005331D7CCEE7EB9CDBBE03CDD94C9448D`; Office `B045AE9D7DB397D24E04DEC2BD1773B4BFA76767611504EE48F2836A6EB35EEC`; CSS `9A83E8B9AEE25CEF5D4999C0731760C833FBA0979813497CB418D3F23B76BEA5`. All joined/runtime/screens remain **NOT_RUN**. These are checkpoint bytes while the root is still editing.

## Final 0414 reader/Worker/Office source closure

**SOURCE PASS for this slice.** Fresh saved-source reread closes W01, W02 and W03; the preceding OPEN table is retained finding history, not the current verdict. No essential outstanding source defect was found within this slice. It does not close the future captured-cause writer review, exact per-Case renderer or the explicitly blocked D05/DR37 dependency.

Current Office1723 starts the visible-window refresh at the first retained block's original cursor and chains each freshly returned `next_cursor`, stopping on fresh completion and limiting requests to the retained maximum three blocks. It publishes the replacement window only after all blocks qualify in one native context. Old overlapping block boundaries are no longer reused. Current2101 refreshes this window on ordinary polls; user-forced refresh resets it. Current2167 and2236 preserve an actual visible row/offset across busy, append/back and poll renders; sort/reset deliberately clears the anchor. Current CSS973 keeps the original two server-sort headings available on phone. Current2253 invalidates the waiting request ticket and disconnects its observer on native dismissal; the existing actor/NEXT/namespace invalidation and native-host guard still govern display and actions.

Waiting remains a live diagnostic, not a frozen monetary selector. A cursor whose exact native tail/Run/WB context has changed is still refused by the server and shown with the retained read-error/Refresh path; no lease token is exposed or adopted. The whole-scope waiting EXISTS starts from this exact Run's original selected membership (0414:89–90), with current-owner point qualification per selected Candidate; it does not scan the Candidate directory. An absence may inspect the selected scope, so no constant physical-scan or latency assertion is made. Original PROGRESS attention semantics and run counter conservation remain identical to2259. The common capacity sentence uses only the recorded instruction Boolean and introduces no browser formula, money authority, route, mutation or confirmation blocker.

| Final reviewed input | SHA-256 |
| --- | --- |
| SQL0414 complete reader/progress successors | `8344BFF1A8ECD96E3FBB289F3B3F16CB37B56E5043B4B9D5999FBF2CE8D7FE9E` |
| Worker preparation | `A57251A62FBBA523BA217F262C936F005331D7CCEE7EB9CDBBE03CDD94C9448D` |
| Office | `60D97083CCDABB60BC774532D9C542C8E6D231D1177CA3810751FD291B6B271D` |
| Office CSS | `9A83E8B9AEE25CEF5D4999C0731760C833FBA0979813497CB418D3F23B76BEA5` |
| Current reader registry | `D3B122DCC0E05712B00A61F8BE8818ECCD7185E9F9E7CA5ADA61D8AF35FCD117` |

0404 helper/migration hashes remain unchanged from the preceding receipt. This reviewer performed source reads/comparison/hashing and this requested documentation amendment only. Source gates, compile/catalogue, app timing, joined behaviour and A4 pixels were not run or observed by this reviewer for these bytes. **Runtime/application/screen acceptance: NOT_RUN.**
