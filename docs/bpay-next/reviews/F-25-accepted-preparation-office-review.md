# F-25 accepted preparation Office: independent source review

9 October 2026. Source review of BUILD-FE `js/bpay-next-office.js`, both existing unit compatibility edits in `tests/unit/bpay-next-office.test.cjs`, and the actual accepted-page/refusal Worker and 2336 SQL contracts. Root alone edits product. No JavaScript/module/browser/runtime, SQL, tests, fixtures or hosted calls were executed by this reviewer. Owner start sheet section 3 controls allowed checks; source review is not runtime or installation proof.

Verdict: **PASS for the fresh saved Office source and both existing unit compatibility edits.** All three finite findings below are fixed and independently re-read. The accepted-page/journal/refusal source agrees with its controlling contract. Final Office SHA-256: `ba5ed0beffb01f0d7c626e8a9d41cf148ed5605cd5602238c335eea482c70a9e`.

## Findings

1. **Closed: accepted Candidate worker type.** Row schema initially used `workerStatus?`, absent from the Office field decoder. Root changed it to existing `worker?`, whose finite WORKER enum matches the Worker contract. Re-read current line 53 and existing enum/nullable decoder: nonnull real PREPARING/READY/REVIEW/etc. now validate.
2. **Closed: preserve original identity on mutation adoption.** Re-read current line 407: `adopt(SEAL_ENQUEUE)` compares both command and sequence with a known `accepted_prepare` and raises `ACCEPTED_IDENTITY_CONFLICT` before assignment/save if either differs. It now shares the accepted-page identity invariant. Existing receipt validation still binds command/run to request. A refusal during adoption preserves the real state's pending bytes and original accepted identity; the local unsaved `next` object cannot clear them.
3. **Closed: visible-only factual polling.** Re-read current lines 493 and 613–624: schedule requires `doc.visibilityState!=='hidden'`; callback checks the same visibility and connected host before a factual read. Visibility change invokes render, which clears any pending timer and schedules only when visible, unlocked and progressing. Close clears the timer and removes both owner and visibility listeners. Resume performs no UUID/enqueue/Review/claim/step/drain; real read dispatch still revalidates current owner and actor through transport.

## Exact accepted page contract and parity

Office requests exactly `ACCEPTED_CANDIDATES {run_id,after_candidate_id,limit}` and `ACCEPTED_WORK {run_id,candidate_id,after_work_id,limit}`; limit remains 1–100. It validates exactly six Worker envelope fields, the eight-field accepted header, the exact 21-field Candidate row and exact 16-field WORK row. Decimal int64 counters/epochs/sequences stay strings; UUIDs are validated then recursively canonicalized before strict keyset order, accepted-row uniqueness and cursor checks. No money or cutoff is recomputed.

The header binds the requested run and bounds enrolled count by expected count. Candidate channel/umbrella, NO_CASE/selection revision, job ID/status/phase, worker ID/status and earlier-command ID/sequence nullable couplings match Worker validation. Earlier sequence must precede the accepted sequence. Progress state/code allowlists and factual states mirror Worker: actual nonterminal PREPARING phases, earlier waiting identity, genuine cancelled/cancelling/failed states, REVIEW worker, and READY only after DONE/READY PREPARE, prepared actual worker, all cuts sealed, SEALED/COMPLETE command and completed enrollment. These reads do not set `can_confirm` or invent a Review revision.

WORK rows bind requested Candidate, share one accepted_input_id, have unique accepted_work_input_id and strict WORK order. Predecessor sequence is earlier than acceptance. Only SOURCE/PROTECTED carries CLEAR/query hash; ordinary rows carry neither. Effective cut exists exactly for SEALED state. Exact next cursor is null only on complete page, otherwise the last validated key; empty incomplete pages, duplicates, wrong run/Candidate, malformed fields and mixed scope fail closed. Parent's separate SQL/Worker reviews own immutable cut/publication proof; this Office lane does not infer it from current Workbench facts.

## Journal v1/v2/v3 to v4 and uncertainty

Actor-namespaced storage remains `cloudtms.bpay-next.v1.<actor>`, with 65,536-byte bound and exact top-level metadata. Complete v1 ALL metadata gains the existing case defaults/prior count; complete v2 gains only unknown channel plus empty Draft addresses; complete v3 gains only null `accepted_prepare`. Partial metadata or extra keys refuse. Adoption retains `pending.bytes` literally, and validates that their parsed action/request serialization remains exact. It does not rewrite pending UUIDs, dates, counts, selected intent or uncertain command bytes. Snapshot freezes accepted identity and pending record copies.

Validated SEAL_ENQUEUE receipt retains its original command/sequence, and accepted page discovery pins the same identity even before all Candidate jobs exist. Existing accepted identity prevents binding/adopting another run. Page reads reject changed accepted command/sequence and a different command from a pending SEAL_ENQUEUE; they do not clear pending bytes. Migration intentionally adds null rather than inventing original identity for old journals; a factual page must qualify it.

First-attempt definite SERVER_REFUSED/AUTH_REQUIRED with outcome_unknown=false may clear pending. `retry(true)` retains uncertain original bytes even if the later response is definite refusal/auth failure; that reply cannot establish the earlier dispatch outcome. Malformed/oversized successful receipt, timeout/network, actor/owner refusal and failed adoption remain pending. Close/reopen retains the actor journal. No unknown outcome becomes a replacement command or new UUID.

## Typed refusal and UI behavior

Typed refusal is admitted only for SEAL_ENQUEUE, HTTP409, exact known-outcome envelope and finite phase/reason/detail schema. BEFORE_ACCEPTANCE reason/retryability mapping matches Worker, including TRANSACTION_NOT_FRESH; WORK requires Candidate. ACCEPTED_IDENTITY conflict binds the requested run and contains original command/sequence information, never a replacement receipt. Malformed typed detail becomes RESPONSE_INVALID rather than a false claim of nonacceptance. Generic errors retain existing outcome semantics and owner-invalidating module codes.

UI renders original command/sequence, enrollment counts, Candidate accepted channel/week/progress and accepted WORK original/effective revision/query/cut states. Explicit original-progress and bounded next/first Candidate/WORK buttons use factual GET pages. Opening/refreshing a sealed preparation reads accepted progress, including interrupted enrollment without requiring a worker row. Automatic timer does one bounded factual page/status read; no REVIEW, UUID, enqueue, claim, step, provider or drain. Review/Confirm remain explicit existing server mutations; accepted READY never grants Confirm on its own. Current choice UI disappears after accepted sealed state; later current approvals are not substituted.

Transport calls `requireActor` before request, after headers and after bounded JSON; ownerReady invalidation and actor change preserve uncertain journal. Close increments render epoch, clears timer, removes owner/visibility listeners and detaches host. Accepted Candidate/WORK loaders reject stale epoch/disconnected-host UI adoption. Polling is now gated on visibility both before scheduling and before firing. In-flight matching factual identity may still be retained in the actor journal after close, which is safe and does not grant UI action or clear uncertainty.

## Existing compatibility edits

Exactly two existing test cases changed: legacy v1 fixture removes new `accepted_prepare` when constructing the actual old metadata; legacy v2 fixture does the same and expects adoption version 4. Both continue asserting exact pending bytes and partial-metadata refusal. There is no new test/harness/data programme in this delta. `tests/unit/bpay-next-office-frozen-detail.test.cjs` has no diff and was not edited. This reviewer inspected the saved edits and executed neither file.

## Final saved-source checkpoint

| File | SHA-256 |
| --- | --- |
| BUILD-FE `js/bpay-next-office.js` | `ba5ed0beffb01f0d7c626e8a9d41cf148ed5605cd5602238c335eea482c70a9e` |
| BUILD-FE `tests/unit/bpay-next-office.test.cjs` | `b59979ae82548952da91de39d1e4c3e24d2798564708becfe1f253a6d5bc9b79` |
| BUILD-FE `tests/unit/bpay-next-office-frozen-detail.test.cjs` (unchanged) | `3ac53acadc4251376f2ffcdc703892414bcabaee8447a2e8837848a1df1e6b89` |
| BUILD-BE `broker/src/bpay-next-preparation.js` | `9271ec0086bffdd9779707bf44407eba0923b1392317eb142e46c8f8e0338749` |
| BUILD-BE `supabase/repeatable/09102026_2336_banking_pay_next_accepted_preparation_progress_v1.sql` | `e019512a0efde50be464513bd60a231d221a3a403f12a80c212ce4e90bc1c2dd` |

Controlling plan: F-25 card `01_BUILDER_RULES_AND_FOUNDATION.md:1039–1057`, R2 §4, saved `design/F-25-accepted-transport-map.md`, owner start sheet §3. Root's reported compilation/seal is separate evidence and is not represented as reviewer execution.
