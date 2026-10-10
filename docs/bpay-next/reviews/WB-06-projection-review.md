# WB-06 position/projection boundary review

Independent source review, 10 October 2026. Read the full WB-06 card (02:247–279), all D01–D23 checklist objects, current WB-02 classifier and WB-08 projection owners, 1430 predecessor bodies, and the saved successor provenance. No SQL, business calls, tests, fixtures, database, hosted or runtime execution was performed by this reviewer.

**Verdict: PASS for the saved 1346 four-function projection-boundary slice. No remaining concrete defect found.** Larger linked-owner fanout remains a genuine WB-44 dependency; joined application, queue and display acceptance is not claimed by this review.

## Source identity

| File | SHA-256 |
|---|---|
| `supabase/repeatable/10102026_1346_banking_pay_next_position_projection_boundary_v1.sql` | `88e129008b29922790832e361e74bdb9bf4ccb7dc3893d7af0c961f59a0f6c70` |
| `docs/bpay-next/final-definer-closures.json` | `27e6e603bfc6de290db463652ea341b48b59431e7fe74610491363b31f27b119` |
| `docs/bpay-next/design/WB-06-successor-provenance.json` | `63d3ff2f5c00377cba515dcb6af4221705df8e97cb610db49dab6f0b02ceb6c0` |

The registry selects the exact four signatures: `mark_processing_v1(uuid)`, `refresh_many_v1(uuid[],uuid[])`, `publish_staged_revision_v1(uuid,uuid,uuid)` and `apply_position_page_v1(uuid,uuid,bigint,text,text,integer)`.

## Reviewed boundaries

**Processing and summary conservation.** Lines 16–35 resolve real NEXT work under Candidate→WORK, capture its prior complete contribution, and advance one coherent projection revision. A false-to-true dirty transition increments the maintained dirty-group count once; repeat invalidation does not increment it again. Lines 36–70 replace the old contribution with a single nonselectable, unknown GROUP_EVIDENCE row and send the exact old/new typed contributions to the existing sole delta owner. Unknown monetary fields remain NULL, with `complete=false`, `certified_zero=false` and `PROCESSING`; this does not forge a zero approval or expose partially updated native positions as Ready. The existing stored withdrawal WAIT marker survives the invalidation (lines 28–31, 62–65). No position, hold, Case, approved input, command or native earnings calculation is added to this helper.

**Publisher authority and clock order.** The 1430 CREATE-body comparison shows only one Candidate variable, the pure immutable replay probe and Candidate prehold, and the exact Processing hook before the original command clock. The original locked replay probe is retained as well. Input, physical Timesheet/current-version/booking, monotonic revision, predecessor and immutable publication checks remain intact. Candidate control is acquired before the existing WORK lock (lines 160–164). Metadata, projection singleton and natural rollups are acquired by `mark_processing` at line 217 before `receive_command` at line 221; no new projection lock is acquired after that clock. The original command member/publication/seal tail remains unchanged. A repeated command returns its own retained receipt before the new metadata/projection work.

**Ordered native pages and final transaction.** Comparing the full apply CREATE body to 1430 shows only the page bound/default and NULL-limit refusal, plus the projection tail. The original lease/epoch, exact expected phase/cursor, immutable publication predecessor, NEW/REMOVED writes, source-basis preservation, per-component collection reconcile, line-count assertion and terminal receipt remain unchanged. Both intermediate passes call only Processing invalidation (lines 430–432), not complete contribution replacement.

Final REMOVED completion writes this publication's APPLIED pointer, DONE job and COMPLETE command first (lines 394–409). The existing financial-view revision increment occurs at lines 420–425. Only then does line 429 run the complete bounded refresh, in the same transaction. A projection failure therefore rolls back that final native page and receipt together. A lost terminal reply returns the original DONE/APPLIED identity at lines 275–281 before any new projection write. The private page default/max is now 100 and an explicit NULL limit is refused (lines 240, 256–258).

**Older final job with newer current revision.** The native predecessor check remains against `applied_revision_id`, not the mutable current pointer; finishing an older legitimate publication never assigns the newer revision as applied. The reused 1332 refresh classifies approved current≠applied work as incomplete/Processing and makes its amounts nonselectable/unknown. Its special reauthorisation branch preserves the stored-before WAIT marker until the genuine effective boundary. This slice does not turn an older job's completion into a newer Ready offer.

**Exact linked Cases and once-only replacement.** `refresh_many` preserves its existing envelope, missing-owner checks and 101-ID sentinel. It combines only explicitly supplied Cases and real `work_collection` links, then preholds sorted Candidate controls, exact WORKs and exact Cases before metadata/singleton work (lines 87–105). Candidate/scope relationships come from the native foreign keys, not inferred unrelated debt.

The new case-before array (lines 107–110) is built over the deduplicated UUID-sorted Case set; the later sorted loop uses the same order. If the work's true WAIT transition already refreshed an exact Case through 1332, its projection revision changes and the wrapper skips that Case. Otherwise the wrapper refreshes it once (lines 111–117). A newly created Case group has a positive revision versus the stored zero sentinel. Candidate/Case locks prevent a concurrent writer from falsely satisfying the skip. The existing real Case refresh always advances that projection revision before replacement.

**Scope and security.** Native earnings, collection, accepted Rule-B selection, capture/hold/PIN owners and parked F25 producer/resolver work are unchanged by this slice. The two new projection helpers use SECURITY DEFINER and explicit `pg_catalog,private` search paths; the original publisher/apply invocation model is preserved. All four functions retain audited logical ownership and revocation from PUBLIC, anon, authenticated and service_role (lines 440–450). No new public interface or trigger attachment is introduced.

## Required remaining ownership

The bounded refresh refuses a combined owner set above 100 rather than silently truncating it. WB-44 remains responsible for the genuinely larger indexed fanout and adoption closure; this source PASS does not assert those owners are installed. WB-41 still owns the actual origin WAIT Case classifier/allocator. Other position/hold/outcome writers require their named W11 refresh ownership and the WB-06 completion census. Real queued processing, lost-wake recovery, per-Candidate isolation, multi-book totals, decrease/increase outcomes and screen Processing remain later joined acceptance, not observations from this source review.

## Follow-up correction: retain the newer publication's dirty barrier

Fresh source SHA-256: `8934065d152ab69bbe2ec8ee298bd03b66f4c20a82fbbcdced78567db37bb69f`. Registry SHA remains `27e6e603bfc6de290db463652ea341b48b59431e7fe74610491363b31f27b119`.

The initial review above missed one real edge: a completed older publication could invoke the general 1332 refresh while a newer approved current revision was still pending. That refresh preserved visible Processing and unknown/nonselectable amounts, but cleared `dirty` and decremented the dirty-group count. Preserving visible Processing alone did not prove preservation of the newer publication's dirty barrier. The earlier assessment of that edge is superseded by this correction.

The corrected apply tail at lines 426–434 performs a full refresh only when the page is DONE and there is no newer/different approved current revision. For an older publication completing behind an approved current pointer, it now calls `mark_processing` instead. The decision uses the actual locked WORK row and this immutable publication's own revision identity; no mutable unlocked pointer or inferred revision is involved. Native applied-pointer/job/command completion still belongs to the older publication. The Processing helper retains `dirty=true`, increments its count only if previously false, advances the projection revision, and preserves the reauthorisation WAIT marker. The later current publication's genuine completion performs the complete replacement and clears that barrier. A withdrawn work can still receive the correct retained applied-fact refresh.

**Follow-up verdict: PASS for this corrected saved source; the older-publication dirty-barrier issue is closed.** No product edit or execution was performed by the reviewer. All previously stated future ownership and joined-acceptance limits remain in effect.
