# EX-01 Office source review — initial findings

11/10/2026. Independent read-only review of the actual saved EX-01 Office helper/progress variant, preview transport, journal/callers, owner-epoch closure and scoped CSS against BD110, full EX-01 card/D01–D14 and [Office design review](C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/bpay-next-build-backend-20261009/docs/bpay-next/design/EX-01-office-design-review.md). **Two essential P2 findings remain in this snapshot; do not claim final source/A4 PASS yet.** Root alone edits product. No test/build/syntax, database/SQL execution, fixture, browser/hosted call, screenshot/render, product/checklist write or commit by this reviewer.

Current snapshot SHA-256:
- FE `js/bpay-next-office.js`: `c08100772e0b16ec65d2d498e606ac965b8164ea199db2e848623ff6c257a6f3`.
- FE `js/main.js`: `dc1c700c19aa0dbbf0ffc06f01d985bfe1909a94430fef2963d1b29028ab8669`.
- FE `css/bpay-next-office.css`: `6cdf177a7c390e0bdac0e95ac3d629e9606b8d40780537ec4e33c43d98b71092`.

## Essential findings sent immediately to root

### P2 — qualify the exact stack stage and the post-await caller before side effects

At [Office1078–1082](C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/bpay-next-build-frontend-20261009/js/bpay-next-office.js:1078), `parentCurrent` checks only `prefix.every(...)`. If the captured parent stack remains but a different child is appended while the preview request waits, this predicate still succeeds and the resolved preview mounts a new credential window above that unrelated child. The original context has not been proven as the current parent/top. During auth `frame.active()` is checked, but the preview-to-window opening stage lacks that check. At [protectedMutation1539–1544](C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/bpay-next-build-frontend-20261009/js/bpay-next-office.js:1539), the post-await predicate checks alive/scope/controller state but not current actor/owner or exact active parent before saving the financial journal.

Smallest fix: retain the same parent scope predicate, require the full original stack length/identities and original top before opening, allow only original prefix plus this exact active auth child while it is open, and require original parent stack/top restored after closure. Recheck `requireActor(options)` / owner immediately after the helper returns and before `controller.mutate`; preserve the existing identity/current intent checks. Do not add a new modal framework or weaken native close ownership. Server proof/action guards prevent a changed actor/action from committing; that does not justify a stale credential mount or journal write.

### P2 — a changed second-factor policy must not turn Send code into action submission

At [Office1123–1126](C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/bpay-next-build-frontend-20261009/js/bpay-next-office.js:1123), START's `tfa_required===false` directly calls `acceptToken`. The submit label was selected from preview.second_factor_required, so a preview that said code-required has a Send code button. If policy becomes code-disabled before START, the native handler reads the new policy and returns a direct token. The helper then closes and its caller dispatches the protected action following a click labelled Send code. That contradicts the accepted design condition that a password-only action button names the action. Economic scope equality alone does not prove unchanged policy: current Worker [index138873–138880](C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/bpay-next-build-backend-20261009/broker/src/index.js:138873) compares the native scope, then independently re-reads second-factor policy and returns the direct token.

Smallest fix: compare START's actual factor path against the displayed authoritative preview policy before token acceptance or advancing the action. On a mismatch refuse/close and obtain a fresh preview in a new deliberate flow; do not dispatch a direct token from a Send code click. START remains final authority, and configured required code / TEST bypass must never be inferred from browser state. No new schema/command is needed.

## Reviewed boundaries which are correct in this snapshot

| Boundary | Source finding |
|---|---|
| Historical tokenless journal | PASS — protected `reauth_token` is an optional closed shape addition in requestBody, so hydration and identical old pending bytes still reconstruct. New mutate separately requires token except retained exact instruction/transfer retrieval. No pending-byte migration or token injection. |
| Command byte envelope | PASS — financial JSON serialization is measured as UTF-8 and capped at 2,048 before journal save; token uses a finite 1,536-byte native two-part token shape. Neither bound is enlarged/truncated. |
| Unknown outcome | PASS — original dispatch(false/true), save-before-dispatch, exact bytes, original action/UUID/operands/token and receipt identity cross-checks remain. A later refusal cannot clear an originally unknown request. No re-preview/re-auth is introduced in existing exact retry. |
| Current entrypoints | PASS — only current CANCEL_REQUEST and first CSV_ISSUE use protectedMutation. Nearby PAYE_NET/TRANSFER_BEGIN/preparation Remove/Discard are unchanged. Future protected owners extend their actual typed schemas/callers, not fake UI branches here. |
| CSV retrieval | PASS — ISSUED_CSV with no retained original instruction gives the agreed inline refusal; no guessed instruction UUID. Retained original pair goes through existing original receipt call without a new proof. Replayed native receipt may truthfully retain payment_recorded; first issue still requires false. |
| Native preview | PASS — transport uses closed nine-field preview plus authoritative policy, exact requested action, one actual Candidate, finite pence total, CASH/DRAFT_GROSS distinction and accepted/scope shape; browser only formats returned exact value and supplies no total/hash as authority. START's expected hash is an independently checked display fence. |
| Password/code | PASS — one active input, native password/code autocomplete and numeric six-digit check; START and VERIFY use original auth routes with BPAY_NEXT_ACTION. Exact finite responses/purpose/token/positive TTL are checked. Malformed/unrecognized error is plain auth refusal, never raw backend output or verification success. |
| Credentials / late network | PASS — child-local password/code only, no journal credentials; dismissal/token completion clears input/password and aborts owned in-flight request; actor/current checks precede adopting replies. Ticket/closed/busy guards suppress duplicate and stale paints. Exact stack-stage gap is the finding above. |
| Parent / progress variant | PASS subject to stack fix — new concrete exported progress variant delegates the same helper and owned native adapter; original parent/cursors/controller persist. No parallel journal or early Release/Execute subsystem is introduced. |
| Current owner epoch | PASS — main7216 `ownerReady` requires both NEXT and the opening owner epoch; proof flow still uses current actor/environment and original API address. No role/authoriser/server permission weakening. |
| Presentation | SOURCE PASS — standard small native adapter, header Close, one active field/primary footer step, wrong-code error alert / results status, secondary resend, native tokens, 44px scoped phone controls, no new list/date/calculator/!important. Actual A4 captures remain pending. |

The helper currently covers the two existing Office protected actions. Reason/scheduled-password-only and future money callers remain with their genuine later action/schema owners; no pre-execute pay date, copied blurred future Execute parent, company value, new Candidate list or early financial feature is required by this review. The separate backend proof/consume/replay reviews remain their own authority.

After root fixes the two guard conditions, re-read exact final bytes/hashes and append a closure here. Wrong actor/scope/action, policy concurrency, expired/unconfirmed token, wrong-code/resend, parent close/reopen and financial/business/hosted journeys remain authored NOT_RUN; static review must not mark them executed.

## Final corrected source closure — 11/10/2026

**SOURCE PASS. Both initial P2 findings above are closed by the fresh saved bytes; their outstanding status is superseded by this section. No essential source defect remains within this narrow Office batch. Actual A4 and runtime acceptance remain pending.**

- Office1079–1083 now separates actor/owner/endpoint/caller/prefix qualification from `parentCurrent`, which also requires exactly the original stack depth before opening. Office1099 admits only that same prefix plus one exact active owned adapter child; `canFocus` uses this predicate. Office1150–1151 accepts a token on dismissal only when the exact original stack is restored. The unchanged native close path pops the closing frame before its dismissal callback (main337189–337192); no native token or close guard was removed. Office1546 repeats `requireActor(options)` after the helper await and before the original identity/current checks and financial journal mutation.
- Office1127 compares START's actual `tfa_required` with the authoritative preview policy before either direct token acceptance or challenge adoption. Resend calls the same checked `start()` path. A mismatch produces the plain refusal and no financial dispatch; Send code cannot silently become password-only action submission.
- Fresh scoped CSS keeps native theme tokens and 44px phone controls. No product changes to journal bytes, financial command bound, retry identity, CSV original identity or the parent controller were introduced by these corrections. The previously recorded PASS boundaries still apply.

Final reviewed SHA-256:

| Saved product file | SHA-256 |
|---|---|
| FE `js/bpay-next-office.js` | `941242941440956619ac3af1384aecfaddb3b41aa54889663668a3774d17f822` |
| FE `js/main.js` | `dc1c700c19aa0dbbf0ffc06f01d985bfe1909a94430fef2963d1b29028ab8669` |
| FE `css/bpay-next-office.css` | `8c977ba6caf434db595df2c66de36e30317fe13e5d24b8fc462e579ce3872543` |

This is independent static source review only, covering the two currently implemented protected Office actions and their concrete shared progress variant. No test, business journey, SQL/database call, hosted request, browser/render/capture or product/checklist mutation was run by this reviewer. Full EX-01 delivery and actual-screen policy approval are not claimed here.

### Fresh presentation amendment and final four-file snapshot

**SOURCE PASS retained.** Re-read the subsequent saved presentation amendment: Office1092–1095 uses existing `data-next-live-copy` markers for initially hidden error/result nodes and the mutable error span; Office1104 changes only that span's text, retaining the decorative aria-hidden icon. The shared moderniser846 exempts only `.bpay-next-reauth [data-next-secondary]` under the exact `bpay-next-read` kind from verb promotion, so Send a new code remains the secondary action. Scoped CSS preserves hidden handling, field/intro sizing and the 44px phone targets. The native helper/context/policy/journal fixes above remain in the fresh source; no financial behavior changed.

This four-file snapshot supersedes the immediately preceding product hashes:

| Saved product file | Final SHA-256 |
|---|---|
| FE `js/bpay-next-office.js` | `4d512b436d7c1f18d22e58a715ef7f9f38dc6b3f3668f56e6396abfddabf2cf0` |
| FE `js/main.js` | `dc1c700c19aa0dbbf0ffc06f01d985bfe1909a94430fef2963d1b29028ab8669` |
| FE `css/bpay-next-office.css` | `01528c7fd67db1da967083f1e94d95dc19805fd0ae1802a3d40d7376ee138e28` |
| FE `js/modal-modernisation.js` | `c07b220d3a58415b55b8bee1a1dcca0df7c0153d562056cfbcbb6f026eb99779` |

Actual A4 captures and runtime outcomes are still separate receipts; this reviewer made only the requested Markdown review update.

### Final A4-driven presentation source amendment

**SOURCE PASS retained, 11 October 2026.** Independently re-read the final presentation-only helper/CSS bytes. Office1099–1105/1141 now names the actual final action and uses native `btn-warn` plus the exact left-aligned footer for Cancel/Write off; the initial Send code step retains its sole primary. Office1110–1112 shows Checking… in the existing status node while input/actions are disabled, with the dedicated neutral/sub tone; a genuine resend-success message remains distinct. Office1150–1152 uses the five one-sentence semicolon error copies required by owner A3g, superseding the supplied two-sentence sample without changing the reason or recovery action. Live-copy/alert/status ownership remains intact.

Every added reauth CSS selector now includes the required `#modal.ctms-modern-modal.ctms-kind-bpay-next-read` scope; subject/input radii are13px/10px; only the owned destructive footer is full width/left aligned; 44px phone target rule and hidden handling remain. Re-read the saved exact stack/actor/owner/policy guards in the same helper: no auth, scope, journal, request bound or recovery behavior changed. Actual final pixels remain owned by the independent A4 receipts.

Final source hashes superseding all earlier presentation snapshots:

| Saved product file | SHA-256 |
|---|---|
| FE `js/bpay-next-office.js` | `529d851a64a2a5d2155a89d5c34492b95984c27d5998dd54b4127e62753ee137` |
| FE `js/main.js` | `dc1c700c19aa0dbbf0ffc06f01d985bfe1909a94430fef2963d1b29028ab8669` |
| FE `css/bpay-next-office.css` | `b7d5c114d5a44d4c5ce3dc76aabab7f07ae0d3c0d5b932479dc15b781d016b9d` |
| FE `js/modal-modernisation.js` | `c07b220d3a58415b55b8bee1a1dcca0df7c0153d562056cfbcbb6f026eb99779` |

No essential source finding remains within the implemented Office scope. This amendment is a source/hash receipt only; no tests, captures, database/business/hosted actions or product changes were made by this reviewer.

