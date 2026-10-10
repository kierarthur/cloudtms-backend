# WB-18 export Worker source review

10 October 2026. Independent read-only review of the exact WB18 D02 Worker route/typed export slice against the full card (02_WORKBENCH:727–746), all checklist rows, R2-F06/W12–W14, saved export investigation, current native1832 choice intake and existing stream/RPC helpers. **SOURCE PASS for this bounded Worker slice: EX01–EX06 are closed at the final source boundary below; no concrete remaining source defect was found.** No product edit, SQL/DB/business/runtime/test/fixture/hosted call or application acceptance was performed. Current SQL correctness and screen/download ownership have separate reviews; this is not whole-WB18 approval.

| Current saved source | SHA-256 |
| --- | --- |
| `broker/src/bpay-next-workbench.js` | `c9b5d2c6116ff8ce12c4b294344989fc829f04be6335adb5d795178263c9f87d` |
| `broker/src/index.js` | `a999b1391a2cf54c7c20828ad4d4fe26d37d26e3cc658df05e9d0aa1e39d8517` |
| `broker/src/bpay-next-legacy-fence.js` | `54aaca5d2de732a4652bd84002160d3061cdd0ad5c336bace3d18fdce3b46fd5` |

## Findings and disposition

### EX01 — CLOSED: ALL Work cannot be combined with component overrides

Fresh source (`580–582`) requires each retained component choice to have an explicit SUBSET Work with the same Candidate/work/revision tuple; contradictory ALL/component descriptors refuse. The ALL export branch then returns true after the unchanged native Work/Candidate eligibility checks (`596`), while SUBSET uses its exact selected line. This closes the following original finding.

`exportIncluded` at590 returns `line?.selected ?? true` for the admitted ALL Work branch. A complete valid descriptor containing selected Work mode ALL and a false component override therefore marks that component Included No. Actual `10102026_1832_banking_pay_next_accepted_choice_intake_v1.sql:349–370,471–473` admits the Work and creates its native ALL/SEALED work choice. Its component membership/eligibility is used for SUBSET (`448–462`); native ALL captures the Work's complete membership. Export must agree with that owner: apply explicit line membership only under SUBSET, or reject an incompatible ALL-plus-component descriptor explicitly. Do not change the existing monetary owner to fit CSV.

Correction of the reviewer's initial recommendation: **a blanket NONE Candidate-default fence is withdrawn.** Actual1832:367–369 permits an explicitly selected Work under NONE without a Candidate override. Only a component by itself cannot promote that Work. Explicit Candidate false remains dominant. The new Work-admission predicate at590 correctly preserves that distinction.

### EX04 — CLOSED: duplicate validation precedes Candidate narrowing

Fresh source creates a separate bounded `supplied` Set for each descriptor kind (`575–576`), checks every supplied ID, and only then filters the map to the requested Candidate. Duplicate IDs belonging to another Candidate now refuse. This closes the following original finding.

At576 duplicate detection uses `maps[kind].has(id)`, but that map is populated only if the row matches `body.candidate_id`. For a Candidate-A export with an original unscoped namespace, duplicate Candidate-B Work/component/Case IDs are therefore accepted, even though the complete immutable descriptor is meant to be structurally validated before narrowing. Example: two identical B Work entries, both outside A, pass the map check because neither is inserted. Validate uniqueness with a finite per-kind ID set over all supplied rows before retaining the requested Candidate's map. This stays inside the existing <=1000-entry/32KiB bound and adds no native lookup or all-agency inventory.

### EX05 — CLOSED: SUBSET qualification is deferred until its full selected-line census

Fresh source excludes SUBSET Work from early Candidate qualification (`643`), records its bounded eligible/invalid state and qualifies it only after the full walk (`649–650`), respecting explicit Candidate/Work false. An empty selected subset cannot activate Case deductions. This closes the following original finding. The final separate-portion correction is recorded under EX06 below.

The new prewalk at641 qualifies a touched Candidate from any READY eligible row of its selected SUBSET Work, without proving that the subset contains a selected eligible line or that every selected line is eligible. Actual1832:448–465 marks that Work skipped with `BPAY_NEXT_SELECTION_SIGNED_LINE_NOT_ELIGIBLE` when the subset is empty/ineligible; the later Candidate capture (`485–488`) excludes skipped Work unless a selected payable Case credit qualifies it independently.

Concrete finite descriptor: NONE, explicit selected SUBSET Work, no selected component lines, and a selected Case deduction. The current prewalk qualifies the Candidate from a Work row, although every Work line exports No. The deduction can then export Yes, despite no surviving selected payable Work/Case credit. A selected true line which exists only in a noneligible section has the same issue. Prove native SUBSET selected-line eligibility before using that Work as the Candidate's payable activator, or explicitly refuse the incompatible descriptor. Keep existing ALL, explicit Candidate false, native Work admission and Case-credit activation semantics; do not rewrite the native owner.

### EX06 — CLOSED: selected-line eligibility uses a positive native witness

Final fresh source (`632,637–638,649–650`) retains only bounded selected approved-line IDs having a `choice_eligible` PAYMENT witness. After the entire walk it invalidates a SUBSET Work only if one of its selected true line IDs has no witness, and qualifies the Work only if at least one selected eligible line exists and every selected true line is eligible. A separate Reserved portion does not erase a genuine Available/Zero witness. The fixed typed `choice_eligible` validation retains READY/selectable/known native portion semantics; export identity validation binds Candidate/work/revision before witness adoption. This agrees with native1832:448–461, preserves Reserved rows as Included No, and closes the following original finding without amount calculation or an all-agency set.

At638, any selected line's row with `choice_eligible=false` immediately adds its Work to `invalidSubsets`. A genuine approved line can have disjoint AVAILABLE and RESERVED (or other informational/blocked) portions. Native1832:448–461 qualifies the selected approved-line identity existentially: it needs a READY/selectable PAYMENT AVAILABLE/ZERO witness, and refuses selected IDs having no such witness. It does not require every portion of that identity to be eligible.

Concrete native mixed portion: a selected line has one eligible Available row and one Reserved row. Current export marks the Work invalid upon its Reserved row, so all the Available line and dependent Case rows export No, although native selection accepts the line. Record bounded eligible selected **line IDs** from any qualifying PAYMENT witness. After the complete walk, invalidate a Work only for a selected true line ID with no qualifying witness. Preserve separate Reserved rows as Included No and all existing unknown/portion amount semantics. This requires no all-agency set, amount calculation or native-owner change.

## Corrections reviewed during this source pass

- EX02 CLOSED: the initially unbounded incoming-body read now races the existing bounded JSON reader against the4-second deadline (`605–607`), aborts its reader and clears its timer. The output walk retains its separate120-second deadline and4-second page deadlines. No unbounded whole-response text/JSON or browser monetary arithmetic is introduced.
- EX03 CLOSED: the initial streaming `payCandidates` Set could accumulate every agency Candidate. The current code removes it: only Candidate IDs addressed by the bounded descriptor are retained in `touched`/`qualifiedCandidates`, and the current row receives a qualification boolean (`625–635,649–650`). Default ALL rows use native current payable eligibility directly. There is no all-agency identity array/cache.
- Native Case activation now follows the actual1832:378–404 dependency: a selected payable Work or selected payable Case credit qualifies that Candidate; mere existence of an unselected payable Work cannot activate a separately selected deduction. The prewalk records actual admitted Work eligibility independently of its component Included booleans. This preserves the native membership dependency rather than rewriting financial selection.

## Passed current source boundaries

The exact POST route is included in the existing authenticated Office/admin block and environment guard before constructing a service RPC (`index.js:198777–198790,198830–198834`). The legacy-finance fence exempts only that exact POST interface. Existing unknown/method/legacy routes, source owners and diagnostic exemptions remain unchanged. `withCORS` passes the response body through without buffering it (`136406–136411`). The fixed existing RPC binds the authenticated actor, READ route class, signal and exact named Workbench endpoint; no credential or public-browser RPC is added.

The immutable output descriptor contains only namespace, IDs, booleans, selection mode and revisions; no browser financial facts. Its actor/agency/epoch/pay-date and revision bindings are checked, Client/channel remain scoped, and Candidate export narrows native enumeration. The prewalk validates retained override identity/current revision and complete presence before CSV begins. All pages must match the original coherent context; a changed context refuses instead of adopting or retrying a newer version. Each typed reply has <=100 rows,128KiB JSON, exact fixed keys, native unique ordered cursor ties and decimal text. Sort key declarations are finite and monetary sorts compare exact decimal strings.

The output walks one bounded page per pull with `highWaterMark:0`; cancellation aborts the current RPC and clears timers. The limits bound rows, total page attempts, bytes and lifetime across the validation/output walk. There is no successful EOF on an RPC/type/context/limit failure: it errors the readable stream, and only a proven final page closes it normally. A consumer must still await the entire download before offering a complete file; current Worker200 stream headers alone are not application completion. This follows the retrieved [Cloudflare streaming guidance](https://developers.cloudflare.com/workers/runtime-apis/streams/) and [Workers best practices](https://developers.cloudflare.com/workers/best-practices/workers-best-practices/); no deployment/type/test tool was run.

The19 verified legacy headings are preserved exactly, with one Included Yes/No field. Text cells are formula-prefixed when their first non-whitespace character is =,+,-,@; quotes, commas and newlines are escaped. The amount column is retained exact validated decimal text, with NULL blank and no summation/coercion/repricing. Dates use the factual date-only operand. No bank field is projected. Break remains deliberately blank and Preview row ID uses the native item identity; those genuine no-equivalent mappings must remain explicit in the completion note. Header-only empty output is valid, while unresolved retained override identity refuses.

## Remaining acceptance ownership

Root owns the actual header/Candidate controls. SQL membership/order/enrichment correctness, full stream-consumer failure/download behavior, real1000-row Included-total reconciliation, and screen/application acceptance remain required under their existing owners. All six concrete Worker findings are closed at the hashes above. No new financial protocol, F25 producer, calculator, fixture programme or server feature is requested by this review.
