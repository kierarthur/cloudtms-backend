# FC-04 sequence104 — independent source review

11 October 2026. Initial verdict: **corrections required; A4 pending**. Read-only product review of the actual completed draft against BD116, the complete FC-04 card at `03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:1084–1107`, local checklist D01–D07 (D06 is the owning build), the existing FC-03 controller/footer and native picker/adapter. Only this review Markdown is written. No product or checklist changes, tests, syntax/build execution, browser/render/captures, fixtures, database/business/hosted calls, dispatch or commits.

## Essential findings in initial bytes

1. **P2 — publish the modified existing assets under fresh cache URLs.** FE `index.html:15,3027,3056` still references the prior Office CSS, shared case-preview and main versions. The modified modernisation asset also needs a fresh URL. A cached old main never exposes the new qualified entry, and a cached old shared footer ignores `onState`, losing the form's busy/unknown/accepted locking. Bump only the affected existing asset URLs and retain the new component URL.
2. **P2 — retain the live form error node.** FE `js/bpay-next-payment-advance.js:42` creates an initially empty error paragraph without the existing `data-next-live-copy` marker. `js/modal-modernisation.js:1291–1293` removes empty paragraphs. Later input/Monday/picker errors then update a detached node. Add that existing marker; no new moderniser rule is needed.
3. **Boundary qualification to close before final PASS.** BD116 explicitly requires exact originating parent and retained endpoint/environment qualification. `js/main.js:12985` retains only actor equality and parent membership; the exact frame/context comparison at12988 is only before opening. The original controller key at Office998 contains actor/Candidate, with no endpoint stored. Actual main's `API` closes over an immutable `BROKER_BASE_URL` (`main.js:40–42`), so there is no current-page endpoint switch through that native caller. A reopened journal on another page/endpoint nevertheless has no persisted proof of its original endpoint. Close the declared parent-prefix/current-context and retained-endpoint contract using the smallest supporting existing-journal mechanism, or record a genuine stronger native invariant; do not introduce a parallel financial journal or infer old unknown intent.

The shared footer currently chooses its live role from `line`, although it may render `inputHint` instead. For an actual known-missing channel the visible “Set a pay channel…” hint can consequently have `role=status`. Use the rendered effective message for the existing alert/status decision when fixing this narrow accessibility detail.

## Source conclusions already established

- The new native creation form fixes ADVANCE/PAYMENT_ADVANCE/NOT_APPLICABLE, uses a genuine Candidate UUID and the actual picker/caller pay-method fact, and never invents the channel for an unknown seed. The picker returns before closing its child; accepting only while the original form is alive and remounting on native parent `onMount` is the correct existing lifecycle. Actual Candidate revalidation is retained in `main.js:288524` onward. An existing edit/Case seed is not converted into a new Advance.
- `nextMonday` uses the Europe/London calendar date and always chooses the following actual Monday, including Monday-to-next-Monday. The entered UK date is checked as a real calendar date and Monday; invalid text is retained. The browser performs input validation and display formatting only.
- Only one repayment operand is submitted: weeks or weekly amount; the other is null. Positive decimal text is not rounded, converted or calculated. Blank threshold/floor remain null and explicit zero remains the exact entered value. Reason and optional Note use the existing closed CASE_INPUT bounds (including2048 UTF-8 bytes), with no invented approval comment.
- Shared Preview validation retains the exact input/context token and server-calculated weekly/count/total/date facts. The new optional callback receives a copy of presentation facts, not mutable controller/token state. Every edit invalidates the Preview; Preview/dispatch lock the form, and accepted recovery hides unavailable captured terms. The “Up to” caption avoids claiming equal instalments or calculating the last instalment. No payout/funding/effective debt is inferred from principal.
- The original Candidate-scoped controller, request builder and receipt validator are reused. A pending request restores terms only from its typed original bytes; a different Case family is refused rather than transplanted. Retrying retains the original command/bytes, including after an earlier unknown result; no automatic fresh mutation is introduced. Accepted recovery uses the existing exact CASE_COMPONENTS read, and empty/failed reads preserve acceptance for Refresh.
- Office `mountCasePreviewFooter:1934–1950` rejects unrelated run/worker or Candidate/pending identity, requires CASE_CREATE for pending recovery, and finishes creation only after the matching targeted Case callback. The form adds exact Candidate validation for returned rows. The explicit NEXT result suppresses the legacy whole Banking Pay refresh; a future FC-02 list/other host is not claimed as built.
- The finite BE correction is sound: command `dependencyError` is passed the actual action and preserves only CASE_CREATE plus SQLSTATE55000 plus exact `BPAY_NEXT_CASE_CREATE_SOURCE_CHANNEL_UNBOUND` as known409. Preparation `safeError` receives the actual read kind via `action ?? kind`; its exact CASE_PREVIEW/CASE_CREATE branch does not relabel generic context failures. Original error semantics, bounded input/output, deadlines, auth, dispatch and unknown handling are otherwise retained.
- Current Preview's context-unavailable branch does **not** prove missing channel. The form's known-missing refusal therefore comes only from the actual Candidate pay-method fact; no generic `NOT_ELIGIBLE` cause is guessed. This source distinction is retained in `FC-04-worker-error-advisory.md`.
- No SQL or money owner is changed. The shared current FC-03 creator/schedule/intake/ledger/funding/queue owners remain current. FC-04 D01–D05/D07 are context; D06 alone is the new deliverable.

## Initial reviewed SHA-256

| File | SHA-256 |
| --- | --- |
| BE `broker/src/bpay-next-command.js` | `91bdfd79d1da7cf62214ee743284aafd471f527308fa995cd0b86c5ce4ebdb94` |
| BE `broker/src/bpay-next-preparation.js` | `40c02030afe7fbf15366d2ea9cfbfd19745a9a8e5c399b85b6bc04156025c646` |
| BE `design/FC-04-implementation-boundary.md` | `8661ce9ae40415530b973f78e6183bdf7c719c9ee0c0e5ce978311244527ccf5` |
| FE `js/bpay-next-payment-advance.js` | `ba20546045b571b7e78bb9a55be67e5f27a11e1b80277f5fe6b8546d5dad2af0` |
| FE `js/bpay-next-case-preview.js` | `f46a7181097643de820594e7ce44ee265d6542c44ad553e0ef1b0a4f0b81e5d1` |
| FE `js/bpay-next-office.js` | `cbdb844f11509a334cab8e87fc0cdc247b6e73ebe3f741ac8cb5be789e700745` |
| FE `js/main.js` | `f90a133f9f543901cce43b98d46329934c26249578cec675eb725cc191be6d4c` |
| FE `css/bpay-next-office.css` | `257783da6796c94398ef0d1e38a68fb7a3aa2f5f7c1d478d72bed8e0cfff6106` |
| FE `js/modal-modernisation.js` | `0cdb7f981c039afd8b86c14124d728cd1ad824cefd18cb294a313393112f642b` |
| FE `index.html` | `6ce9f10303f8fe183a6070773bbbf97150cbfe894d988e08e1aab01cc3748deb` |

These are initial source-review bytes, not deployment or app evidence. A final amendment must replace the open findings after rereading the corrected source. Pixel/A4 and joined real create/idempotence/funding/queue/host effects remain separate pending evidence.

## Final source closure — corrected current bytes

**SOURCE PASS; no remaining essential source defect found in this bounded FC-04 batch. A4 remains separate/pending.** This amendment supersedes the initial open verdict and findings above.

- All affected existing cache URLs are now `20261011-fc04-r1`, including the shared native adapter. The new form error uses the existing live-copy marker, and the shared footer selects alert/status from the displayed effective message. These concrete findings are closed.
- Main retains the exact original stack prefix and originating parent's `_ctxRef`, in addition to the original actor and positive qualified NEXT epoch. The owning form requires its own live frame; active dispatch/Preview/read/result paths also require its own active frame and generation. Picker return is accepted only under the same retained form/actor/owner before child Close and remounts on native parent return. A late unrelated actor, ancestor replacement or parent context cannot invoke the owning return callback.
- Root clarified the scope: cross-page endpoint metadata is not a card/checklist deliverable and must not broaden the original FC-03 journal or protocol. The real native API is a const closure over the current page's canonical broker address. The original canonical actor/Candidate journal and exact accepted/pending bytes remain unchanged. I amended BD116's two overbroad endpoint-assurance phrases to that exact invariant and explicitly marked cross-page endpoint-change joined acceptance NOT_RUN. The original endpoint distinction is consequently a documented limit, not an invented additional proof requirement or source PASS claim.
- The shared native adapter's only new behavior preserves its already-owned body content and footer nodes when they are already installed. It removes only other marked footer/header detail nodes, still attaches the correct original nodes after a native child return, and retains existing stack/Close/cleanup/focus behavior. The shared footer similarly preserves its existing two button nodes when already present. These changes prevent unnecessary detach/reinsert while retaining genuine remount and callback guards; they do not alter command/controller state. Root's actual-click evidence remains A4 evidence owned by root, not execution by this reviewer.
- Fresh complete current source and diffs retain the original typed request/token/receipt validation, same-input Preview gates, exact unknown retry and accepted targeted read. The optional state callback copies presentation facts and fields lock during busy/pending/accepted states. No new financial authority, browser schedule formula, automatic request or money owner is introduced.

Final reviewed SHA-256 (earlier hashes above remain historical only):

| File | SHA-256 |
| --- | --- |
| BE `broker/src/bpay-next-command.js` | `91bdfd79d1da7cf62214ee743284aafd471f527308fa995cd0b86c5ce4ebdb94` |
| BE `broker/src/bpay-next-preparation.js` | `40c02030afe7fbf15366d2ea9cfbfd19745a9a8e5c399b85b6bc04156025c646` |
| BE `design/FC-04-implementation-boundary.md` | `2b65d648ffc954f113fd18a82b354a70ac897f369b0c8488012a129a746c03d8` |
| FE `js/bpay-next-payment-advance.js` | `6e2903cccc1115d638a68d75bca1cc64e2b39f853a8945717e55704716e8c733` |
| FE `js/bpay-next-case-preview.js` | `4e6392861710877998c78234ec2ab8efd8cb6110d6c650cbdad9b9d16ca92dfd` |
| FE `js/bpay-next-office.js` | `cbdb844f11509a334cab8e87fc0cdc247b6e73ebe3f741ac8cb5be789e700745` |
| FE `js/bpay-next-ui.js` | `718c2ac4bd4827cbdd07218fa846db5ccfaf9f5457fea0ff7ef7b4fcc8911462` |
| FE `js/main.js` | `2c47ef647af8b98911db16483de87bd8f83ee3e858d65e81d2f325f21a05abed` |
| FE `css/bpay-next-office.css` | `257783da6796c94398ef0d1e38a68fb7a3aa2f5f7c1d478d72bed8e0cfff6106` |
| FE `js/modal-modernisation.js` | `0cdb7f981c039afd8b86c14124d728cd1ad824cefd18cb294a313393112f642b` |
| FE `index.html` | `402770bd4343761a178e95087fdf7b05e8d1078f36e33973ccb50e19f96d0b70` |

This is source-only approval of the corrected narrow batch. Actual pixels/policy, deployment and genuine create/double-click/recovery/funding/queue/parent update observations are not supplied by this review. They remain separate A4 or AUTHORED_NOT_RUN acceptance.

## A4-driven source supplement — inline error routing

Fresh read of the presentation corrections retains the original actor/owner/parent/generation, typed Preview/Create, journal and targeted-read behavior. The accepted Candidate subject is outside the hidden form and uses the real retained label. Pending/accepted hides Preview and retains the sole Try again/Refresh action; scoped native CSS respects hidden. The existing live error marker, effective message roles, node-preserving adapter/footer and conditional form remount remain sound. Worker and SQL behavior is unchanged.

One new **P2** must close before the amended source PASS: `payment-advance.js:65` maps every generic “Check the entered details” refusal to Reason and positively sets its `aria-invalid`, even when the actual invalid operand is Note >2048 UTF-8 bytes, weeks above bigint, or monetary text beyond the existing1024-byte bound. Shared `case-preview.js:8–16` correctly rejects those inputs, but a valid Reason is falsely marked and the inline error is placed at the wrong field. Preserve the existing finite bounds in the form's presentation validation and route the corresponding actual field; leave an aggregate generic refusal unassigned instead of blaming Reason. No financial derivation or schema change is necessary.

Supplement reviewed SHA-256: form `3166bdefed236a1f3667f0295da60c9360a3597af11c935bc2861e410155c7a7`; shared footer `580d3ed723e873e7f289a85c9bda1174cfe7f463529862703b76e2eb6073d0f3`; CSS `739146aa05caaa7215b273bbce1ec58ee09b5ddba236f1468e738dc8de679dad`; BD116 `417386c6cd7cb6045065efba16b43c87106e6f0989acc96b77567bd2ae462f8b`. Other product hashes remain those in the prior final table. This supplement temporarily reopens only the concrete new inline-routing source point; it adds no joined/runtime or pixel approval.

## Final A4-driven source closure

**SOURCE PASS restored; all source findings above are closed or superseded by the explicit scope limit.** Fresh complete form reread confirms the exact original numeric1024-byte bounds (ASCII after decimal validation), bigint weeks limit, Reason/Note2048 UTF-8 bounds and null-character rejection. The guarded bigint conversion runs only after valid whole-number text. Note now participates in clearing stale error attributes and receives its own relevant inline error. Generic aggregate refusal remains at the form without falsely setting any field invalid. Case-insensitive Start week routing also places the exact Monday refusal at that input, preserving its existing hint association.

The retained live error node, one inline message, footer duplicate suppression, real Candidate subject outside the accepted hidden form, pending/accepted hidden Preview and scoped hidden CSS are sound source changes. Native `onMount` preserves a current shared view and refreshes its presentation rather than recreating its buttons/ticket; actual Candidate changes still invalidate/destroy and remount the original Candidate-scoped controller. Actor/NEXT/parent/own-frame/generation, exact typed input/token/request/retry/receipt and targeted Case completion remain unchanged. No remaining essential source defect found; no product execution or A4 pixel claim is made by this amendment.

Current final SHA-256 supersedes earlier source tables:

| File | SHA-256 |
| --- | --- |
| BE `broker/src/bpay-next-command.js` | `91bdfd79d1da7cf62214ee743284aafd471f527308fa995cd0b86c5ce4ebdb94` |
| BE `broker/src/bpay-next-preparation.js` | `40c02030afe7fbf15366d2ea9cfbfd19745a9a8e5c399b85b6bc04156025c646` |
| BE `design/FC-04-implementation-boundary.md` | `417386c6cd7cb6045065efba16b43c87106e6f0989acc96b77567bd2ae462f8b` |
| FE `js/bpay-next-payment-advance.js` | `cccc07b702b6de26af6e9a13d2423c910dbfbde5e9fb74dc0eacb19f93a5d46b` |
| FE `js/bpay-next-case-preview.js` | `580d3ed723e873e7f289a85c9bda1174cfe7f463529862703b76e2eb6073d0f3` |
| FE `js/bpay-next-office.js` | `cbdb844f11509a334cab8e87fc0cdc247b6e73ebe3f741ac8cb5be789e700745` |
| FE `js/bpay-next-ui.js` | `718c2ac4bd4827cbdd07218fa846db5ccfaf9f5457fea0ff7ef7b4fcc8911462` |
| FE `js/main.js` | `2c47ef647af8b98911db16483de87bd8f83ee3e858d65e81d2f325f21a05abed` |
| FE `css/bpay-next-office.css` | `739146aa05caaa7215b273bbce1ec58ee09b5ddba236f1468e738dc8de679dad` |
| FE `js/modal-modernisation.js` | `0cdb7f981c039afd8b86c14124d728cd1ad824cefd18cb294a313393112f642b` |
| FE `index.html` | `402770bd4343761a178e95087fdf7b05e8d1078f36e33973ccb50e19f96d0b70` |

A4/policy remains the separate screen review. Joined real Create/idempotence/unknown recovery/funding/queue/parent-update and cross-page endpoint-change observations remain NOT_RUN unless their own authorised evidence is supplied.
