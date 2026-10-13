# First-install Office integration review

13 October 2026. Independent read-only investigation/source review. **FINAL SOURCE PASS — narrow Office merge; OI-01/OI-02 closed.** The final resolved bytes and hashes are recorded at the end. Earlier conflict/open checkpoints below preserve review history. Root alone resolves product. This reviewer performed only saved source/Git/document reads and writes this receipt; no messages to recipients, SQL/RPC, network, tests, render, product edits or Git mutation.

The first-install owner directions explicitly require merging main's contact-policy/review flow with NEXT's original request/busy behavior. Do not restore the old direct outreach handler over the new review flow, and do not invent a new communication protocol.

## Exact original authority

- Main `js/weekly-source/combined-review.js:10–58` owns `contactPolicy`, `contactPlan`, `contactSignature` and per-recipient `contactOutcome`. Eligibility requires the actual typed contact policy plus enabled native owner authority. Grouping remains publication/cycle-owner-specific, even where destinations coincide. Queued means a durable message intent; it does not mean delivered. UNCHANGED/aggregate-only replies are insufficient to clear a selection as newly queued.
- Main's `bindCombinedReview` owns audience, eligible selection, fresh bounded review, included/excluded summaries, explicit confirmation, one recipient/owner at a time, fresh pre-dispatch eligibility/signature and per-recipient outcomes. Preserve all these behaviors.
- NEXT `sourceRequestContext` captures original frame/open generation, external actor-current callback, selected context and filters. `sourceRequestCopy` clones native inputs; `issueCommand` serializes the original action/payload. `isSourceAdmissionBusy` accepts only HTTP409 + exact WEEKLY_SOURCE_PAY_QUERY_ADMISSION_BUSY + retryable true + failed_rpc_outcome ROLLED_BACK + automatic_retry false.
- `buildOutreachRequest` reuses the original reader-owned actor/cycle/publication/workspace/proof and narrows exact group keys. It does **not** create a request UUID.
- Current registry selects `public.weekly_source_office_bulk_query_action_atomic_v1(jsonb)` from `15092026_1534_weekly_source_read_projections_v1.sql`. Its closed six keys are actor_user_id, source_cycle_id, projection_publication_id, expected_workspace_version, action and selection. Lines5118–5146 check current publication/workspace version and selection proof before the communication owners. This is not a durable command receipt/readback service.
- Existing native dedupe is at message membership/generation: candidate identical membership returns UNCHANGED (delivery source4256–4263), submission request retains unchanged native membership (3440–3466), manager generation retains unchanged native membership (3678–3684). Current native settings, eligible incidents, complete cohorts, actual routes and the separate manual-reminder cooldown remain server-owned. They do not justify describing a lost reply as delivered, queued or safely absent.

## Minimal combined behavior

Retain main's review/confirm handler. Add only narrow per-recipient retained state inside that original review transaction:

1. Capture the review/audience and original current predicate. Keep the existing per-recipient index/outcomes; do not rerun previously qualified queued recipients on retry.
2. On each **first attempt**, use main's freshContacts, contactPolicy and exact contactSignature checks; then build and clone that recipient's native action/payload before dispatch. No typing, selection or review operation sends contact requests.
3. An exact admission-busy envelope proves that attempt rolled back. Stop at that recipient, retain its original payload and original review, and expose the same confirm control as an explicit exact-request retry. Never rebuild pending input from a fresh reader or a changed audience.
4. Once a rolled-back request has been retained, **every subsequent failure retains its exact input** (original NEXT `busy || retained` behavior). A later transport failure may have committed; do not turn it into proven absence just because the retry's current error differs. Its copy remains unconfirmed/status-check-qualified. There is no automatic resend.
5. An initial non-busy unknown result follows main's existing per-recipient failed/unconfirmed presentation and explicit request-status/fresh-review remedy. Do not create a new UUID, receipt poll, persistent communication journal, or unknown-readback route. A later fresh review uses native eligibility; it must not be described as replaying the old command.
6. Accept per-recipient outcome only through the existing contactOutcome qualification. Preserve earlier queued/skipped outcomes and delete selection only for qualified queued rows. Replace/update that recipient's outcome on exact retry rather than duplicate the totals.
7. While in flight, disable every other actionable control. While a retained retry is waiting, allow only the original confirm retry; audience, selection, review/back/dismiss, filter/tab/sort, row actions and scrolling-triggered reads cannot replace the original offer. Retain existing current/frame/actor checks before and after every awaited fresh read/dispatch/repaint. No stale callback may write into a newly opened workspace.

The native outer version/proof check can refuse an exact retry before the generation dedupe is reached. Therefore the receipt must not claim arbitrary lost-reply recovery is guaranteed. Keep the original payload and the honest unconfirmed result; this merge does not authorise server-owner changes. The existing open lifecycle is in-memory and clears old pending state on a new open; no new durable cross-close recovery claim is made.

## Index and non-conflict retention

Preserve the full current NEXT script set and dependency order, including resolution/adoption assets, and the main contact-policy combined-review/weekly-source CSS changes. The loaded combined-review remains before import-workspace. Cache URLs must cover actual merged bytes, retaining SP10/NEXT Source-retirement/admission/native-window additions rather than replacing them with older main URLs. Retain the non-conflicting imported-expense/current Source changes and unrelated saved work.

## Checkpoint identities / limits

The checkpoint contains unresolved conflict bytes and is not runnable SOURCE PASS. Original main/HEAD and exact saved hashes are recorded below; a final amendment must re-read root's resolved bytes and close only the narrow combined behavior above.

```text
[{"name":"index.html","hash":"DC6585E597CCF22C43D4C10738869EA2D26017A6F7747CF10A299172C7B2A5E7"},{"name":"js/weekly-source/import-workspace.js","hash":"B4240CDB8879E3533E1B21798C85211BB0E4278E789AC58187E152CA5B48F294"},{"name":"js/weekly-source/combined-review.js","hash":"EB6B70427F10C5C1964B3D7F179595A006C7A6286482954AAB067BF4F23314E1"},{"name":"css/weekly-source.css","hash":"BAB589D85D17B9EBA6477A081DDB51D859C87F6DEF56B89F585E519DC3DD158C"}]
612a3251019ca9ebe3a37ce697637882043dc683
887e7af7cd15d9f4034ad73806b44344f3eb0791
```

No first-install release, provider/message delivery, joined acceptance or test result is inferred. No communication was sent by this reviewer.

## Resolved-byte source checkpoint — two findings

Root saved conflict resolution. Main contactPolicy/plan/audience/review/signature/per-recipient outcomes and fresh first-attempt eligibility are present. Confirm retains the exact cloned payload/rows only after the proven rolled-back busy error, keeps nextIndex and earlier outcomes, skips freshContacts/build for that pending recipient, and keeps later retained failures. No request UUID, new command, readback service or automatic resend was added. Index preserves the HEAD NEXT asset set/order with main contact-policy assets and new combined-review/import-workspace cache URLs.

**SOURCE remains OPEN for OI-01/OI-02:**

- **OI-01:** initial contact dispatch sets state.busy/inFlight, but the final lockSourceRequest receives null until an admission-busy catch retains the request. Main's combined-review actionButtons and data-wsr-action handler do not uniformly guard state.busy. Native row Open/actions can therefore remain clickable during that first dispatch. Pass an ephemeral inFlight lock for state.busy when no retained request; this locks existing controls without creating a persistent unknown journal or new protocol. Idle retained state must still permit only its original confirm retry.
- **OI-02:** initial Review's freshContacts checks requestCurrent before/after its awaits, but the surrounding Review catch/finally unconditionally write session.error and repaint. A stale old Review can therefore display its failure in a new actor/open workspace after currentness is lost. Qualify plan/error/final adoption with the original requestCurrent, preserving the existing bounded reader and review flow.

Both were reported to root immediately. Confirm's analogous error/finally paths are already qualified; no wider business owner rewrite is required.

SHA-256 at this checkpoint: index.html 68391866C3996D1D9A0511DB1F1B07073184B153594D5B4F9A3DEE2D21451A78; import-workspace.js CC44A5ABDA2360C6E150C44F5D8A3B4BC7D84D8BA36465C7A54F83355EB70ED2; combined-review.js EB6B70427F10C5C1964B3D7F179595A006C7A6286482954AAB067BF4F23314E1; weekly-source.css BAB589D85D17B9EBA6477A081DDB51D859C87F6DEF56B89F585E519DC3DD158C.

Source-only static review; no syntax/test execution, network, communication or product edits were performed by this reviewer.

## Final resolved-source verdict

**SOURCE PASS — OI-01/OI-02 CLOSED.** Fresh saved bytes were independently read after root's repairs. There are no merge markers in the two conflict files.

The transient lock now uses `state.pendingSourceOutreach ?? (state.busy ? {inFlight:true} : null)` on the existing host controls. Initial review/confirm therefore locks those controls without persisting an unknown request; an idle retained operation permits only its existing confirm retry. Original inFlight and current guards prevent duplicate dispatch. Review's plan adoption, catch error assignment and final repaint are qualified by its captured requestCurrent; a lost actor/frame/open does not paint an old failure into the new window.

The final confirm handler retains main's original review/audience, exact fresh per-recipient policy/signature check, included/excluded summaries and contactOutcome accounting. It freezes each first payload/rows immediately before issue. Only the exact proven ROLLED_BACK busy catch starts retained retry. Retained retry resumes the same nextIndex/request payload without rereading eligibility or rebuilding the pending input. Earlier completed outcomes remain; selected keys clear only on existing qualified queued results. Later retained failures keep the original request. Initial unknown outcomes preserve main's status-check/fresh-review copy; there is no automatic resend or invented original-command readback.

Index's complete HEAD NEXT scripts/dependency order remain, with main's contact-policy CSS and combined-review changes. Combined-review loads before import-workspace; both merged modules use the new 20261013-contact-policy-next-r1 cache version while Source admission/native-window additions remain. Unrelated original Source/finalise/rate/recheck behavior stays as the HEAD side.

| Final saved FE file | SHA-256 |
| --- | --- |
| index.html | 68391866C3996D1D9A0511DB1F1B07073184B153594D5B4F9A3DEE2D21451A78 |
| js/weekly-source/import-workspace.js | 03FBD654AA5D139F737E6169F8E51003BD72F516E608B2465E9C9F7469268A76 |
| js/weekly-source/combined-review.js | EB6B70427F10C5C1964B3D7F179595A006C7A6286482954AAB067BF4F23314E1 |
| css/weekly-source.css | BAB589D85D17B9EBA6477A081DDB51D859C87F6DEF56B89F585E519DC3DD158C |

Parents: HEAD 612a3251019ca9ebe3a37ce697637882043dc683 and origin/main 887e7af7cd15d9f4034ad73806b44344f3eb0791. Earlier conflict/finding checkpoints above are historical, superseded by this final verdict. Root separately reports syntax checks PASS; this reviewer ran no checks. Source PASS does not assert message delivery, joined business acceptance, installation or cross-close durable recovery.
