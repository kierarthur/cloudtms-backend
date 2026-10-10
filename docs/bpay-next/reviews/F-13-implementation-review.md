# F-13 implementation independent source review

10 October 2026. **PASS — no remaining concrete source finding in the reviewed D01–D03 slice.** This is a source/code/document review, not installation, hosted queue, activation or app acceptance. No product edits, module execution, tests, SQL/DB, fixtures, external calls or secret reads were performed.

Read the complete F-13 card (foundation 748–767), three checklist rows, checked F-13-SCOPE/BD022, native qualification investigation, full supporting module, index import/shared-consumer diff, existing NEXT background/financial adapters, actual legacy producers/descriptors, certificate status/schema, original completed-Draft owners, F-18 category reader/predicates, audit schema/ACL/current saved RLS contract and D03 recovery procedure.

## D01 exact admission and preservation

The classifier requires a plain record and enumerable data properties, complete exact raw ten/eight/thirteen-field legacy shapes, numeric versions/sequences, bounded values and timestamps. It uses the existing closed operation source-policy map without adopting the legacy parser's coercion/defaulting behavior. Extra/mixed fields cannot enter a legacy qualifier. NEXT discriminators select only the existing adapters; those still enforce the complete metadata/financial schemas before any RPC and retain invalid inputs. The existing NEXT financial adapter has the same 2048-byte envelope ceiling, so the classifier's ceiling introduces no larger accepted NEXT shape that bypasses its validation.

The shared consumer preserves one-message/interface admission, reads fresh current owner before classification, and retries under LEGACY/DISABLED. It adds no old advance, claim, drain, recovery, provider or mail call. Every unqualified legacy return and caught failure explicitly retries. An unavailable/malformed owner, damaged batch or unknown envelope has no new ACK path. Actual methods are called on the platform Message, preserving their receiver.

The proposed timestamp issue in the first captured module (`65ff14e9…`) is **CLOSED in the fresh saved source**: both regex branches now explicitly constrain hours 00–23 and minutes/seconds 00–59, while the calendar check refuses normalized invalid dates. The producer branch requires the actual millisecond-Z shape. A `T24:00:00.000Z` legacy envelope no longer classifies. This review did not execute a reproducer.

## D02 positive native facts precede an exact receipt

The certificate path calls only the existing STABLE status reader with the envelope's actual session/actor, validates all 21 keys, exact session/certificate identity, current-ready SEALED_CURRENT/V2, positive safe native versions/counts and explicit ready/not-pending/not-recovery flags. The real status owner selects the exact current session version/progress/fence certificate. Existing schema binds its retained certification identity to the retained digest. The path retires this completed build wake and makes no claim about Draft/payment completion.

The operation path is confined to SELF/DRAFT_CREATE. It requires one exact actual completed operation, matching nullable envelope batch/root identities, no active owner markers, FROZEN/seeded native source scope, real source session/snapshot and a finite positive complete candidate-scope count. The bounded `limit=101` scope read must equal the stored count (at most 100), have unique native IDs, matching operation/session/source tuples, actual batch IDs and ALLOCATED/DRAFTED states. It enumerates every distinct bound batch, including a matching nonnull top-level batch. Every batch must positively appear exactly once in the current-epoch F-18 LEGACY_BATCHES page as OBSOLETE_BOOKKEEPING/nonblocking. Absence, EMPTY, off-page identity, summary readiness and one qualifying batch among several cannot authorize ACK.

This reuses the original completed-Draft owners: `09082026_1128` validates the frozen selected universe, real batch provenance and completed chunks before writing COMPLETE and clearing lease/lock; the V8 terminal owner likewise validates its row-backed universe before completion. F-18's existing unused-batch predicate positively requires an actual unsubmitted unused batch with no admitted money/provider/settlement/advice evidence. The existing terminal descriptor returns NONE for completed Drafts; the separate money-child-to-root wake branch applies only to settlement/remittance. No child/root financial obligation is inferred closed by this Draft rule.

The first F-18 category page is intentionally a bounded proof subset. A batch beyond that page remains retained even if it might qualify on a later page. This is conservative transport behavior, not evidence that it is absent. Money operation kinds, ROOT/CHILD and all Workbench SESSION/CANDIDATE/GLOBAL wakes remain unqualified and retry. A ready certificate for another or incompletely identified scope is not adopted as their closure. No new SQL/evidence framework is introduced to broaden those families.

The transport is fixed to service-role headers, fixed table/RPC paths, capped 64 KiB streamed JSON, redirect refusal and a complete eight-second abort deadline. It exposes no lease, provider/bank payload or arbitrary exception body. Supported native 128-bit hex/UUID-shaped queue IDs map directly to the audit UUID; the complete original message ID is retained in the compared packet/object/correlation fields, so representation/case collisions cannot silently adopt another receipt. Unsupported IDs retain.

Actual positive qualification occurs before every audit lookup, including replay. The path only GETs/INSERTs the existing audit table with a closed bounded native-ID/disposition packet. It does not upsert, update, delete or use the best-effort generic audit helper. Existing-row replay requires exactly one complete matching selected row. Successful INSERT or HTTP 409 alone is insufficient: a fresh awaited GET must exactly match before returning retired. Lost/failed/unverified responses retain. Index then rechecks fresh NEXT owner and the same epoch before its sole legacy ACK. The unique audit event supplies the once-only bounded reason log; console exactly-once and global audit immutability are not claimed.

## D03 bounded DLQ procedure

`F-13-DLQ-RECOVERY.md` correctly names the unchanged TEST queue/DLQ and saved wrangler values (batch 1, timeout 1, retries 10, delay 5, concurrency 2). It distinguishes explicit 30-second retention and financial 120-second lease-wait from expiry evidence. DISABLED exhaustion preserves durable NEXT jobs, rediscovered by F-11 only after separately authorized resume/F-26 epoch treatment. Actual old obligations require their real F-18/F-19 disposition; unsupported/unknown/nonterminal/incomplete identities stay retained.

The procedure requires reviewed identity/commit/owner/quiet-window evidence and bounded authorized inspection, preserves exact validation and receipt-before-ACK, and grants no purge, manual ACK, infrastructure change, reverse activation, stale nonce adoption, old recovery engine or payment/provider/mail action. A republished transport ID receives its own receipt only after requalification. Hosted/DLQ/activation/app observations remain NOT_RUN. F-18/F-19 quietness and genuine retained dispositions remain separate release requirements.

Existing source gates being added by the root were not executed or yet reviewed in this code verdict. They do not alter the source PASS boundary above.

## Fresh saved-source hashes

| Source | SHA-256 |
| --- | --- |
| `broker/src/bpay-next-legacy-retirement.js` | `8c0da37eccedf87173fe9942c630817d4f97fec09e91e356b82eec1a66764e24` |
| `broker/src/index.js` | `57893c36c37e2ea69c01a3cc8d11dcbf35ed655f89bc258c3a4eb690a1ad1825` |
| `docs/bpay-next/F-13-DLQ-RECOVERY.md` | `bf7445e54715b1d276baaf8c91f693d3306e55300d270d2e4dde8e40e53bbee9` |
| `docs/bpay-next/design/F-13-native-retirement-qualification.md` | `0a53a4ed906e5db7e4d8bfa0e3e01dd20a5b07b672d3f5dba7dec0eb0398a2d8` |
| `09102026_2109_banking_pay_next_cutover_readiness_page_v1.sql` | `980a79151e8cc49f3db8c5c4e1f8cd99aac6039600405e25ec1803d3b7c91add` |
| `09082026_1128_banking_pay_operation_finish_post_draft_authority.sql` | `d90ee3bd93f2da774046980e68a808dcb0c98e7bbf4cb25b6f2010d8a391cd1d` |

The status/schema/audit hashes remain bound by the preceding design/native investigation receipts. No reviewed SQL was changed by F-13.

## Finite existing source-unit case review

Read the complete saved `tests/bpay-next-background.test.js` and its Git diff, including the three appended cases. No case was executed by this reviewer. Root reports the normal gate 31/31 PASS; that is root-run evidence, not independent runtime evidence. Fresh test-file SHA: `0381b7cbd8c97293156d7e98b3aa0a189b9d6254936b450ddaf6d2e39b89a9ee`. The product hashes above remain unchanged.

The diff adds only the retirement-module import and three finite cases to the existing normal source-unit file; prior cases/assertions are preserved. The classifier case uses actual raw families and rejects numeric coercion, missing/wrong values, extra keys, 24:00 hour, invalid calendar date, unknown source, wrong ROOT/scope and overflow depth. The injected transport case uses the actual certificate-status argument/reply contract, fixed audit paths, redirect refusal and signal, models a committed INSERT whose reply is lost, then requires requalification/exact read-back of the same native message ID without a second POST. It confirms lease text is absent from the stored packet and a different retained certificate UUID cannot be adopted. The third case requires no transport call for an unsupported message ID or an unqualified global Workbench scope. All are data-free in-memory response stubs; no database/queue fixture, hosted call, new harness or separate verification program is introduced.

**F13-CASE-01 (case quality only, open at this captured hash):** the pending-certificate assertion is evaluated after `stored.after_json.certificate_uuid=NONCE` has already made the receipt mismatched. It would still return false if the pending qualification guard were accidentally ignored, because the preceding mismatch independently refuses the replay. Restore the matching stored certificate UUID before setting `certificate_build_pending=true`, or assert that the pending call performs qualification only and never reads/inserts the audit. This is a bounded assertion-isolation fix inside the existing case; no product defect or extra case/harness is requested. Product source PASS is unchanged, but the requested pending-case protection is not independently demonstrated until this correction is read.

**Final follow-up: F13-CASE-01 CLOSED; finite case source review PASS.** Fresh saved test SHA `a085d6d5f930b29f01e6ef482a9be0571ad4231f645048c51f478c41f1decdfd`. The existing case now restores `stored.after_json.certificate_uuid=JOB`, captures the event count before setting the pending status, and asserts that the pending attempt adds exactly `['qualify']` with no audit read/INSERT. Thus a mismatched receipt no longer masks the pending refusal, and ignoring the pending guard would violate the event assertion even if the outcome changed for another reason. No prior case or assertion was removed; no extra harness/DB fixture was added. Root reports syntax and all 17 background cases PASS after this correction. This reviewer fresh-read the correction only and executed nothing. Product source hashes/verdict are unchanged; **no open F-13 source/case finding remains**.
