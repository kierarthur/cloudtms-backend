# DR-16 Worker and Office contract review

12 October 2026. Independent read-only SOURCE review of the newly saved `broker/src/bpay-next-preparation.js` and FE `js/bpay-next-office.js` mappings against current0442 native reader, full DR-16 card and R3. No product edits, SQL/business calls, tests, rendering or joined acceptance. The initial receipt is saved promptly: **changes required** for W01–W03 below. The financial-owner review remains separately recorded in `DR-16-totals-review.md`.

## Essential finite qualifications

- **W01 — worker context completeness.** Shared `reviewResult` Case branch (initial BE:61 / FE:252) requires its worker/Candidate/status but permits NULL preparation revision, captured preparation revision, Case selection revision and both NET revisions. Native context supplies all five for every nonnull worker. Require this exact nonnull tuple in REVIEW_CASES; totals-only context correctly keeps all worker operands NULL. No extra clock or context field is needed.
- **W02 — frozen-offer coherence.** PREPARE_ISSUES Candidate validation (initial BE:102 / FE:293) permits `frozen_offer_complete=true` with NULL frozen gross, or a PREPARING/CANCELLING/CANCELLED row that has NULL gross but known deductions/channel. The native flag means READY/REVIEW/DRAFT/ISSUED/COMPLETE with complete frozen money. Require those states plus nonnull frozen gross, deductions, channel and worker when true. Zero remains known; genuinely incomplete REVIEW remains unknown.
- **W03 — native penny and capacity shape.** REVIEW_CASE amounts are nonnegative native penny fields, while the reused general `money` type permits negative values and six fractional digits. Scope the new row to at most two fractional digits and nonnegative due/this-Draft/outstanding/reduction. Require NULL or zero reduction to have NULL reason; positive reduction must have its closed foreign-capacity reason. This is the exact existing0404 instruction constraint, not a new money calculation. It also prevents a six-decimal number being incorrectly interpreted by the two-decimal numeric-sort comparator.

## Matching source already reviewed

Both mappings add only REVIEW_TOTALS(run_id) and REVIEW_CASES(run_id,worker,sort,after,limit) to the existing page caller. The new shared shape is identical in Worker and Office. Original active Office/admin/service authority, actor injection, endpoint/caller path, deadlines and uncertainty handling are retained; these are read pages, not accepted commands or new retries. JSON after cursors are serialized/parsed through the existing bounded query seam; duplicate/unknown query members reject. Cursor uses exact original context and native component UUID, 8KiB closed bound; ordinary request stays32KiB and the original streamed response byte/deadline bound remains.

Totals money accepts the native numeric(38,2) width. BigInt penny comparisons only verify the server's two maintained channel rows and whole-row conservation; no browser display amount is derived from member arrays. Channel order is exact PAYE then Umbrella, revisions match context, count inequalities and gross ex/VAT/inc conservation are checked. First refusal is the same closed three-code minimum. These are validation operands, not financial authority.

Case pages validate sort value against its actual row value, native UUID tie, nulls last in both directions, PostgreSQL C/UTF8 text order and exact penny numeric order. Duplicate returned component IDs reject; next cursor equals the last returned native key/current context; continuation advances strictly. Excluded rows cannot claim instruction/result/this-Draft amounts. Unknown outstanding remains NULL. Counts retain native selected+excluded=scope identity; no client total or reconstructed allocation is emitted.

PREPARE_ISSUES extends its existing closed sort enum with CHANNEL and DEDUCTIONS and carries both maintained channel revisions. New row facts are native channel/deduction/frozen qualification/Case count; original skip/release/expiry/discard bindings and waiting-owner checks remain. STATUS/CHANNEL_STATUS adds only the native gross-stage deduction, reserved recovery and complete-frozen qualification keys; its projection/reservation identity logic remains. Known REVIEW-context/not-ready conflicts are action-qualified; unrelated error handling and mutation journals are retained.

Current raw BE diff contains mixed-line-ending churn (646 additions/583 deletions at initial read); builder notified to preserve unchanged raw lines. Review findings concern normalized logic. This is a packaging advisory, not a financial behavior claim.

Initial saved SHA256: BE preparation `7c5f8557f43c6dae07cd90ccc67fd3ed857bb3dcd22fa39b62cb8c10d5ffd48d`; FE Office `49ee3387f125db7301d9eb54846dd2ccd73ca03c02203ba3fc88e8b9fdb7706c`. Fresh corrected-source hashes/verdict must supersede these before closure. UI/A4 and runtime are outside this initial mapping receipt.

## Fresh corrected mappings — SOURCE PASS

The actual saved Worker and Office mappings now close W01–W03. W01 explicitly requires all five worker revision operands in the Case context. W02 permits a complete frozen offer only in READY/REVIEW/DRAFT/ISSUED/COMPLETE and requires its native worker, gross, deductions and channel; known zero remains valid. W03 uses a new scoped nonnegative `reviewAmount` with native16-integer-digit/two-fractional-digit bounds for the four Case money facts, and binds the capacity reason exactly to NULL/zero versus positive amount. The numeric comparison therefore cannot reinterpret a six-decimal financial amount. Other scopes retain their existing money types.

Independent static comparison confirms the complete7518-character shared review contract block is identical in Worker and Office after line-ending normalization. Its original scope/cursor/conservation/error/authority checks remain as described above. No remaining essential source defect was found in the closed mapping scope. UI implementation and screenshots are not approved by this mapping receipt; no SQL/tests, runtime or joined scenarios were executed by this reviewer.

Current independently read SHA256:

- BE `broker/src/bpay-next-preparation.js`: `757d3098338b1077be1bf7e5790b8cb44ab359cb928ab3d9d840adf15bf670a4`.
- FE `js/bpay-next-office.js`: `868bb1e22ee63c6ca8c729e47c19b9ffa40c3a70d36650e630a076f43c6df8e1`.

The raw-line-ending packaging advisory remains; a later raw-byte-only restoration needs a fresh hash amendment, not another financial implementation.

## W04 finite-dispatch correction — SOURCE PASS

The builder's local capture exposed a gap missed in the earlier mapping review: the new generic `startsWith('review')` dispatcher intercepted existing REVIEW's `reviewPhase`/`reviewReason` (Office `review`/`reviewReason`) and returned false before their original enums. That could reject the existing financial REVIEW receipt. The current saved correction replaces the prefix dispatch in both callers with precisely17 new types: reviewAmount, reviewMoney, reviewRun, reviewSelection, reviewWorker, reviewCaseSort, reviewNetRefusal, reviewDirection, reviewStage, reviewBasis, reviewForeign, reviewContext, reviewKey, reviewCursor, reviewCases, reviewTotal and reviewChannels. `reviewCounts` remains its separate exact typed shape. Nullable handling still delegates to the same concrete base type. The existing REVIEW output schema and its original PREPARING/REVIEW and ENROLLMENT_INCOMPLETE/WORKERS_INCOMPLETE enums are now reachable unchanged. W01–W03 and the closed review block remain intact.

Fresh source reread verifies W04 closed; scoped SOURCE PASS is retained for these corrected bytes. The reported capture is builder evidence, not an independently executed test. Office presentation wording changes are outside this contract amendment; UI/A4 remains separately reviewed.

Current SHA256: BE preparation `e4fbe1813d7b171765bfc0350fb5c17f5d50dc630e86be704b0d5b580c05fc00`; FE Office `c5ff9d358f0bf1fa569ea3a52182b9516b8c81294d9c7cd4d6abde243380fe3b`. Raw-byte-only EOL restoration remains pending a final hash receipt.

## Final packaging amendment — SOURCE PASS

Fresh static reread after the builder's EOL repair confirms the same7518-character review block remains identical in Worker and Office after line-ending normalization. Both exact17-type dispatch lists, separate reviewCounts shape and original REVIEW phase/reason enums remain intact. The final Worker Git diff is72 additions/8 removals; its changed hunks contain only the previously reviewed contract additions, PREPARE_ISSUES/STATUS fields and qualifiers, two page kinds, JSON-cursor parsing and action-qualified error/result branches. The prior unrelated raw-line churn is gone; the packaging advisory is closed. No product logic correction was added by this review.

The small NX104 wording uses the retained server `net_recovery_reserved` fact and labels it “Recoveries from PAYE net” for PAYE versus “Recoveries at net” for Umbrella. It introduces no formula or changed money operand. The full screen/A4 review remains separate. Builder-reported source gates, db:check/local compile/catalogue/isolation/export/seal/coupling results are not reviewer-executed business or runtime checks.

Final independently read raw SHA256:

- BE `broker/src/bpay-next-preparation.js`: `4c6b2846b38f1d14a835fc9c62e666106370881a9898562bae6d3affecb1c443`.
- FE `js/bpay-next-office.js`: `d6e6f914e1a83994ebe0ca2082e6f2456a41c765445cc1e9c2afd8d94bb2b185`.

Scoped SOURCE PASS retained; W01–W04 and packaging are closed. No SQL/tests/business calls, hosted execution or rendering were performed by this reviewer.
