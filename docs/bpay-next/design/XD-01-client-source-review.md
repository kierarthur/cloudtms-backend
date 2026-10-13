# XD-01 client source review

Independent read-only reviewer, 13 October 2026, BD179/seq147. This receipt covers the independently inspected Worker/Office sorted CURRENT_STORED_CREDITS contract and Stored one-off credits caller at the hashes below. Only saved source and text hashes were inspected. No test, SQL/RPC/business invocation, database, browser/render, network, fixture, rehearsal, Git mutation, product/checklist/BUILD_LOG edit or deployment was performed. Root alone implements and executes checks.

**SOURCE PASS for the reviewed XD01 client checkpoint.** No remaining concrete source defect was found. This is not whole XD01 completion, A4, installed-catalogue, performance, release or real business evidence. During this receipt's save guard, Root's subsequent XD02 Office/action and Case List binding changes became visible. Those new bytes are explicitly outside this receipt: Office B8D2F6E170AF0A6477D288E7F03B02BA0B2F35BF8C6F89517E0EA4DD95D5C10F and Case List 29885808E94926F64D132691211839A337E6722A4AF4025CD96EBC0A00505716. Worker and source-gate hashes remained unchanged. XD07 retains the original oversized-row obligation.

## Closed transport and native tuple

Worker bpay-next-preparation.js:781 adds optional sort only to the existing CURRENT_STORED_CREDITS read kind. Duplicate/unknown GET keys refuse; eight CREDIT/CREATED/AMOUNT/PAID_TO ASC/DESC strings are closed by storedCreditSort. Original Candidate/afterUUID/limit validation remains. Sort absence keeps the original unsorted schema/decoder. The authorized GET uses the existing public preparation page, with no new route, command, binding, wake or financial owner.

The sorted Worker decoder at817–828 validates exactly rows/complete/scan_cursor, bounded rows, exact Candidate and eleven native facts, unique IDs, native raw UUID progress and sorted tuples. Empty nonterminal raw windows are valid. Sorted modes deliberately do not require increasing UUIDs or compare returned UUIDs with raw scan_cursor. The wrapper keeps the same native UUID after_legacy_case_id, without browser-tagged cursors or full bank names in requests.

Both Worker and Office validate returned order rather than sort a loaded page. Created preserves all six native timestamp fractional digits and offsets through existing microsecond helpers. Amount uses exact decimal BigInt comparison. Paid to uses existing UTF-8 byte comparison matching native C order. PostgreSQL UUID text supplies the stable final tie; DESC reverses the whole tuple. Credit is a constant native label and consequently uses the UUID tie. Native amounts are numeric(12,2); these comparisons derive no economics and aggregate no money. Names remain complete native strings within the existing whole-reader/response envelope, with no new producer/name cap.

Office storedCreditsResult at1108–1122 validates the closed Worker envelope, page limit, cursor/complete parity, Candidate, lower-case duplicate identity, native timestamps and tuple order. Old UUID monotonicity constraints remain only in unsorted mode. createTransport.page selects the optional closed sort shape only for this kind. Existing actor checks before fetch and after receipt, bounded JSON body and request deadline remain. Worker retains its 128KiB read response, 32KiB request and 8s dependency timeout. No measured latency or 5s top-level statement claim is made.

## Caller, lifetime and continuation

Reviewed openStoredCredits at1124–1184 freezes Candidate and actor. Its own guard requires the live native frame, current caller, ownerReady and unchanged actor. visible additionally requires the active frame and visible document. Request start, paint, row action and continuation are guarded. After await, lifetime/generation/sort are rechecked. One successful hidden reply is held unadopted; resume validates it again under current guards. Dismissal invalidates generation, removes focus/visibility listeners and clears timer/pending state.

Sort changes use the server sort, reset native UUID cursor, retained rows/highlight and scroll to the top. Append rejects repeated IDs across retained pages and a first new tuple not strictly after the last retained tuple. A cursor-change Refresh calls load(false), so it cannot retry the vanished UUID indefinitely. Other append errors offer scoped Retry. Exact SQLSTATE/message errors are preserved only for this nonmutation read kind, without arbitrary database text.

The caller makes one request at a time, limit20. A visible near-tail nonterminal window continues after raw exclusions, including an empty returned page. Loading remains until native completion, preventing false No credits. Continuation rechecks guards and yields after each ten reads or five-second burst, then may begin another bounded burst. It stops on error, completion, pending hidden reply or a tail outside the trigger distance. These are per-request/per-burst bounds, not total-row/lifetime caps. Native mount, focus, visibility and scroll resume the same reader. Native adapter onMount is queued after frame creation; caller also resumes after frame assignment.

At the reviewed checkpoint, rendered copy contains complete native beneficiary, masked last four, native date and original amount. It does not expose bank hash, full account or internal UUID as copy. The caller is exported and wired through Candidate Cases More at Case List161, using existing child options and current native parent frame. Its optional action callback was not yet wired at that checkpoint; the subsequent XD02 action/binding join is excluded above.

## Historical findings and closure

| Finding | Initial issue | Reviewed disposition |
| --- | --- | --- |
| XC01 | Requests/paints omitted document visibility; awaited reply could paint hidden. | CLOSED: explicit visible guards and unadopted hidden reply validated on resume. |
| XC02 | Ten empty raw reads could leave false No credits and an unreachable next tail. | CLOSED: nonterminal Loading and guarded continuation with ten-read/five-second yielding bursts. |
| XC03 | Cursor/oversized-row native refusal collapsed into generic errors. | CLOSED: exact read-kind/nonmutation/SQLSTATE/message mappings at Worker1117–1120. |
| XC04 | Refresh reused the failed append cursor. | CLOSED: cursor-change Refresh resets cursor and generation with load(false). |

Cumulative retained rows grow as continuous scroll reaches the last native row, as in the existing native Case List. The traced XD01 card, SDP34 and PF01 do not prescribe a mandatory retained-row ceiling for this screen. PF01 bounds each read's rows/bytes and forbids browser financial aggregation; SDP34 requires whole-list server sorting and deterministic indexed keysets. A cap or reusable virtualizer is not required by this review. Cumulative DOM/memory cost is unmeasured; no bounded physical index-visit, memory or runtime-performance proof is asserted.

## Reviewed source receipts

| File | SHA-256 |
| --- | --- |
| BE broker/src/bpay-next-preparation.js | 51CFC44CE1F852792601FA2BD8D89CFC2D2196AC7A5D32F1C04A69049352E797 |
| FE js/bpay-next-office.js, reviewed XD01 checkpoint | 085A96194FCA1921E4BA35D1B3A7E4E71C3137636BC53CA895EE54F17671F0B9 |
| FE js/bpay-next-case-list.js, reviewed XD01 checkpoint | D67CAE3FE04BB77CBDFBCC7ACE83FAC63F7E9C523AC91C7944A86616E26D1E14 |
| FE js/bpay-next-ui.js, retained native adapter | F535E7E75579428CA7B97B57D406CCAEADAEBBF0FE15E5935574F435B772248D |
| BE scripts/bpay-next-source-gates.mjs | 43331432430040D6795B91A849D40EFE6136018D6DDAA13702CAFE6C7DF18FB5 |

Named source-gate admission closure is appended to XD-01-server-source-review.md. Root-reported local PG17/check results remain separate root execution evidence, not independently reproduced by this reviewer. Joined A4 list/action acceptance remains separate and pending this receipt.
