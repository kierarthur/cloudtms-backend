# F-25 rule-B Office correction review

10 October 2026. **PASS for the finite Office dormancy correction.** No actionable source defect found. Independently read the entire saved `git diff HEAD -- js/bpay-next-office.js` (7 additions/25 deletions) and current caller/guard/progress sections in FE build. This is source review only: no tests, JavaScript/runtime, SQL, fixtures, product edits or hosted calls. No whole F25/F11 or installed/app verdict.

## Accepted-page call paths are dormant

Current controller.page at456 refuses exactly ACCEPTED_CANDIDATES and ACCEPTED_WORK with existing local REQUEST_INVALID **before** options.transport.page. It does not send a request, fabricate a response or change other page kinds. The existing posttransport identity-adoption branch is intentionally unreachable for these kinds. Accepted row/input schemas, normalizer/decoder and receipt/journal/refusal parsing remain saved as dormant references.

Both actual loadAccepted/loadAcceptedWork functions and their local page/cursor state were removed. No remaining source reference to either loader or acceptedCandidates/acceptedWork state exists. The toolbar accepted-progress button and Candidate/Work accepted panels with all their paging/open buttons were removed. No ACCEPTED_CANDIDATES/ACCEPTED_WORK caller occurs in another FE `js` file. Actual Office page reads go through the now-guarded controller; the retained low-level transport decoder is not called directly by another actual Office consumer.

Refresh/reopen/exact-retry no longer enters an accepted reader. Current refresh520 loads ordinary worker status or WORKERS/CHANNEL_WORKERS under existing run/channel state and preserves current-selection reads only before sealing. After successful SEAL_ENQUEUE at544 the UI retains the exact receipt and existing local reset, then reads WORKERS. It creates no additional command, Review, enqueue, claim, PIN or drain operation.

The progress timer598–599 now selects only loadStatus or loadWorkers. Accepted progress predicates are removed; ordinary PREPARING/net/transfer/CANCELLING predicates remain. Existing read/owner locking,2s bound, document-visibility check at scheduling/firing, connected-host check, actor validation through act and close/event cleanup remain unchanged. Server expiry is not moved into the browser. Explicit Review preparation remains available; it is not automatically invoked by the timer.

## Retained contracts and bounded scope

The HEAD diff leaves request/reply schemas, D03 typed definite-refusal/unknown-result parsing, exact pending JSON bytes and retry identity, v1/v2/v3→v4 journal migration, actor binding, SEAL_ENQUEUE accepted_prepare receipt identity and identity-conflict checks unchanged. No saved journal/pending bytes are deleted or rewritten. Ordinary current-work/case selection, channel Drafts/status, payment actions, advice and history are outside the changed hunks.

No Worker route, SQL owner, producer/enrollment/PIN guard,0280/1610/1620 or runtime configuration is changed by this Office diff or this review. The accepted readers remain unavailable from the Office flow while Worker/scaffolding source stays retained under rule B. Keeping those declarations does not claim the parked accepted-cut protocol installed or complete.

## Saved source provenance

| Source | Identity |
|---|---|
| FE build HEAD used for complete diff | `f01c311d3058cb8864a1495aeec9652e782796fa` |
| HEAD Office Git blob | `65e665494c88ce1aecbe68dec9f2fadf3f0f221b` |
| Current saved Office SHA-256 | `ad62e30d80660757529b402e0f260f263b57184d56da2dd4dc9f89595acadd9c` |

Reviewed target: `C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/bpay-next-build-frontend-20261009/js/bpay-next-office.js`. Reviewer changed only this Markdown artifact. Source PASS is not runtime or app acceptance.
