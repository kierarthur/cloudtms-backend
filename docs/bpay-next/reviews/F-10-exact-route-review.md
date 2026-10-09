# F-10 exact route source review

Independent read-only review, 9 October 2026. Re-read F-10/F-10-D01–D06, start sheet §3, B11, the saved F-10 provenance, all bounded import/route changes and their surrounding dispatch, the retained F-09 classifier, the three complete ported source-unit files, and the advice diff against saved RESET. No product/test/SQL edits, tests, mock execution, database/SQL execution, runtime, Docker or hosted calls.

**PASS for F-10-D01–D06 source wiring after correction.** The first review found raw-path dispatch inconsistent with the classifier's normalized exact exemptions. Root added a scoped `nextPath` normalization for duplicate/trailing slashes, used it in the predicate, UUID advice matcher and five literal dispatch checks, and retained global raw `p` for unrelated routes. Re-read the full corrected block and all affected source selectors: normalized aliases now enter the same exact authenticated handler; wrong methods and unknown paths retain their fence classification. The sole source finding is closed.

Final reviewed index raw SHA256: `4ad6c296eb400f71d15258a7362042e9b606a17972b1794eb3ef505181363659`; advice: `010d06386af89983e48c63a3eafcc2796eba929f0aa74def33202157b55c00c4`. Both match the refreshed saved F-10 postimages. Its index preimage `8e3b8c8384241e55dde39a02dfcad3bb760f5469967b1c1405ef046c22c68834` is the previously reviewed F-12 final postimage.

## Accepted source findings

| Row | Exact contract / review |
| --- | --- |
| D01 | POST history-page calls the fixed history reader with trusted Office actor; no owner activation guard, queue send, job drain or money calculation |
| D02 | POST frozen-detail calls the bounded frozen reader and fixed page RPC |
| D03 | GET preparation-page calls the bounded preparation reader and fixed page RPC |
| D04 | GET advice with anchored UUID instruction ID calls the original frozen advice renderer and fixed advice RPC |
| D05 | POST preparation uses the closed command handler; only an accepted response bearing the exact ENROLL header requests a metadata wake |
| D06 | POST command uses the closed financial-intake handler and its existing accepted-response metadata wake |

The six-route admission block sits after the F-12 global financial fence and before the older Banking prefix/admin/Workbench contract dispatch. It calls the existing `requireUser(env, req)` without role filtering so a missing/invalid/inactive/session-version-invalid identity returns 401, while an authenticated non-admin returns explicit 403. This occurs before service RPC construction or business RPC execution; the existing active-user authentication lookup is necessarily retained. Each closed handler receives `requireOfficeUser: async () => user`, and its fixed transport receives the existing configured base URL/service headers. No browser actor, generic RPC selector or alternate authority is introduced. The database owners independently recheck service/admin authority.

The unchanged classifier individually exempts exactly the six method/path shapes and the two POST bank-name override/clear facts. PUT, other wrong methods and unknown NEXT financial paths remain classified as old financial routes. Under NEXT/DISABLED/owner-status failure, F-12 provides its deliberate typed retirement/disabled/unavailable refusal before those paths can reach an old writer. No blanket NEXT prefix exemption was added. Corrected local normalization now aligns the exemption and dispatch for duplicate/trailing-slash aliases.

History retains 1206's explicit trusted active-admin read contract without NEXT activation. Its caller has bounded section/ancestry/cursor/privacy validation, actor injection and lossless string/null scalars. No command/preparation activation policy was copied onto it.

Preparation/command wake bodies and their awaited handler responses match RESET. Wake promises return the actual queue `send` promise, attach a static bounded failure warning, and enter `ctx.waitUntil(wake)` with the receiver intact or are awaited if unavailable/throwing. Failed transport does not reject an already accepted durable receipt. Reads do not request wakes. These source semantics follow the current primary [Workers best practices](https://developers.cloudflare.com/workers/best-practices/workers-best-practices/) and [Queues JavaScript API](https://developers.cloudflare.com/queues/configuration/javascript-apis/) guidance checked during F-09. F-11 still owns background runtime wiring.

The scoped index diff contains the bounded new route imports/admission/dispatch alongside the previously accepted F-12 changes. Current upstream Candidate provider checks, MyTMS delivery receipts, Source/invoice internal routes and unrelated bodies are retained; RESET's whole index was not copied. F-12 D05's external quiet-window mechanism remains pending and is not supplied by these HTTP routes.

Advice differs from RESET in exactly three visible-label lines: the net-additions description and LOAN case kind/subtype display as **Payment Advance**. Stored LOAN kinds, schema validation, money values, frozen constituent selection, escaping and all other economic/data contracts remain unchanged. Historical explanation/reference facts are not rewritten. This satisfies B11/OR-42 while preserving OR-45's stored compatibility scope.

## Source gates and remaining verification

Read all three complete existing `tests/*.test.js` ports: command-rpc, frozen-rpc and payment-history-caller. They use local synthetic operands/injected fetch functions and source/registry reads, without database fixtures, seed sets, proof registers, runner programmes, real network calls or imported index runtime. Compared with RESET, changes only adapt source selectors to the actual method-qualified routes and strengthen history's explicit 401/403-before-business-RPC/source-order checks. They are permitted normal source-gate unit regressions under start sheet §3/B11, not the prohibited testing programme.

Read root's final `.codex-local/F-10-source-gates-final.log`: 41 tests, 41 pass, zero failures, including the frozen-advice batch. The checks were not independently executed here. Re-read the normalization correction, refreshed provenance and affected selector changes before this final sign-off. Installed and app-tested remain **NOT_RUN**. Typed inactive financial responses, live auth refusals and all later app acceptance are not claimed proved by this source review.
