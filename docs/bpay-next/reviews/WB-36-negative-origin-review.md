# WB-36 negative-origin draft source review

**PASS for the requested negative-origin and fact/deferred-membership source subset; F1/F2/F3 are closed. Not a whole WB-36 PASS.** The original checkpoints below record the findings and their subsequent fresh closure. No runtime or installation status is inferred. No SQL/runtime/test/business/network execution or product edits.

10 October 2026. Full source reads of the debt/basis, reconcile/origin/observation guard, available origin pool, Case-hold reservation, negative effect and write-off attachments, plus capture sealing/classification. Static full-definition comparison against current2018 reconcile and1519 origin/guard, and exact native0151 Case preparation,0316 recovery,1647 write-off source order. Rule B accepted revision/pin authority is retained; no parked F25 protocol.

## Finite source findings

| Area | Result and actual evidence |
|---|---|
| Per-key algebra | debt_basis computes each actual `(work,component,segment_key)` separately: negative `max(C-A,0)`, free recovery `max(C-A-W-H,0)`, outstanding `max(C-A-W,H,0)`. Here C is signed realised source effect gross; H is only ACTIVE recovery Case-hold origin reservation; W is retained write-off origin gross. Active positive WORK hold is separately retained as h and never counted as realised debt. Reconcile takes SUM of outstanding, then adds actual recovered and forgiven counters. The max stays inside each stable key, so forgiven/protected A cannot hide independent B. |
| Mixed same-parent example | Paid A40/current A20+B60 gives A negative20, B negative0; current parent approved80/realised40 does not erase the20 recovery. Captured positive B60 and recovery A20 explain parent residual40. Once A20 is genuinely recovered, its signed effect lowers C and the actual recovered Case counter retains the completed20. No NET or bank cash ratio is used. |
| Same-key multiple paid origins | available_origins uses actual realised hold-share UUIDs only, with signed effect_origin consumption minus origin-specific write-offs and ACTIVE recovery reservations. It pools only inside the same work/component/key. UUID-ordered cumulative available gross is capped by the key's current free debt before it becomes a parent allocation prefix. A full key amount cannot be recovered twice by selecting another old origin. No synthetic baseline, current ordinal guess or target ratio. |
| Moved parent | Existing stable segment remains one run_work_segment; each original component has its own parent child. Current approval enters only its actual current component. A retained old component obtains zero current approval and exact prior origin evidence; realised credit stays on that old parent. A new day gets its actual positive approval. Source retains its real SOURCE component, and segment_collection_basis explicitly admits the ordinary PAYE segment route rather than turning Source into a DAY parent. |
| Complete native gross qualification | collection_basis requires ordinary sealed PAYE SHIFT or genuine certified zero, exact applied revision and native PAYE source basis, nonnegative actual child approved/realised amounts, exact child A/C/H sums to native position, exact PAYE target tuples, and exact write-off/ACTIVE recovery sums to real Case counters. Missing historic child attribution cannot become an invented zero. The approved line, if present, remains WORK/TAXABLE. |
| Reconcile preservation | Full static2018→1100 function diff contains only the new complete segment-basis fetch, exact complete alternative for genuine old-parent absence, complete-key principal expression and two observed segment fields. All original owner locks, job scopes, original paid anchor, Source pairing/head qualification, Case/component counters, immutable events and same-job replay remain. Unsegmented native predecessor calculation remains its existing branch. |
| Origin / observation preservation | Full1519→1100 origin diff adds only qualified complete segment evidence and exact observed negative total. Existing real original effect/line/revision/transfer/valuation identity and Case rule/component shape remain. Original guard body is retained; its new observation fields must equal the actual helper. The original first INSERT may retain its native anchor with default segment flags, followed by the genuine same-owner observation UPDATE; it cannot claim complete positive evidence on that initial exception. |
| Capture sealing | Current capture defers ordinary Case classification while capturing exact child members. Last member verifies native source A/C/H and actual retained target C/H aggregates before setting complete. Only then does it bind actual collection/origin/reconcile revision and update financial-resolution/expected-managed counters once. Added guard permits classification changes only at same-member complete seal, checks actual collection complete/negative total/revision/work/component/current observed gross, and refuses unexplained release of a financial-resolution flag. |
| Case-hold insertion order | Native0151:1061–1080 inserts genuine typed DRAFT allocation_result and ACTIVE Case hold while Case/component locks are held. Its capacity_use INSERT1081–1083 and active counters1092–1097 occur later. New reservation attachment correctly validates the actual result/hold/FINANCE_ALLOCATE owner and original origin BEFORE requiring later capacity/counter state. It allocates exact native PAYE source target `(S,0,S)` and proves complete source sum against the actual hold. Deferred closure must validate the later native capacity/counter relationship. |
| Recovery effect order | Native0410 realises Case counters/hold/capacity before0316 inserts WORK_COLLECTION_RECOVERED; operation_item_id is actual RECOVERED event ID, whose operation_item_id is the real Case hold ID. New attachment checks that actual event/transfer/Case tuple, REALISED hold/capacity and exact signed parent effect gross. It groups the actual reserved origins per key into negative effect_share, then links each exact origin and proves the whole parent gross tuple.0316 subsequently changes native WORK realised counters and calls reconcile. |
| Write-off order and nullable event fields | Native1647:325–327 emits minimal WRITTEN_OFF event `id=command_id`, original Case and `operation_item_id=work_collection_id`, before328–331 updates forgiven counters and before337–341 terminal request/job/command. New attachment checks exact requested command/Case/component revisions and live owner, then allocates actual free paid origins and reconciles sum to written_off_delta. It does not require nonexistent recovery instruction/result/source fields on this minimal event and creates no financial_effect or C reduction. |
| Cancel / expiry | ACTIVE reservation joins actual Case-hold status. Existing RELEASED cancellation/expiry stops H without deleting immutable children. Signed negative effects preserve actual realised recovery; W remains separate. No new owner command, agency clock or journal is introduced. |
| SQL shape / feature scope | `%rowtype` fields match native hold/result/instruction/write_off_request/event and1100 columns. Numeric SUM/LEAST/GREATEST prefixes preserve pennies; tuple comparisons use IS DISTINCT FROM. Exact collection composite FK already exists in1637. New attachments are private feature hooks on private Case hold/effect/event, private invoker helpers with explicit owner/revokes; frozen public Source bindings and shared valuer are untouched. |

## F1 — origin-leading access paths

`available_segment_recovery_origins_v1` performs three correlated sums using only `origin_share_id`: effect_origin signed source, write_off_share forgiveness, and recovery_share reserved source/status. Current primary keys begin respectively with effect_share_id, case_event_id and case_hold_id. Current auxiliary indexes begin with work/component/key; these do not give the query an origin-leading lookup. The loop can scan unrelated rows for every genuine origin as history grows.

Small necessary support recommendation: add one `origin_share_id`-leading index on each of those three feature-owned tables (optional trailing effect_share_id/case_event_id/case_hold_id). This changes no financial authority, schema field or protocol. It is an access-path finding from source, not a measured performance result. Review snapshot has no such index yet.

## Pending source closure, not waived

The builder explicitly has negative fact/commit guards in progress. They must admit only the real same-scope native owners and reconcile exact whole reservation/effect/write-off membership, including complete capacity state after native Case-hold INSERT. The negative financial_effect AFTER INSERT seam is BEFORE0316 updates the WORK position: avoid treating the temporarily old native position plus newly inserted negative shares as a current complete origin assertion at that intermediate point. Guard admission should bind the actual posted Case/hold/capacity/job and retained reservation; final reconciliation/commit checks may then require the completed native position/Case tuple. No whole-owner, installed or runtime approval is claimed here.

## Source identity

Raw reviewed repeatable SHA256: `0cc9cd19fb403ae7db98c0547077bcf6cbf232f28a793fb4b16878686271adbd`. Raw migration SHA256: `0a2e8a72f43bd6de767e0b277c8c895e02ed29482588a40c23cd51ebd8ac5ef9`. Concurrent builder additions can change the whole draft file; exact requested definitions bind this review:

| Definition | Snapshot lines | UTF-8 CREATE through $function$; SHA256 |
|---|---:|---|
| `private.bpay_next_work_segment_debt_basis_v1` | 4026–4046 | `64eb33d53338ab8bb4585027dd6924b7093f8e4a0b89cec2485a286558e1d26a` |
| `private.bpay_next_segment_collection_basis_v1` | 4052–4083 | `018fa2b1cf3bc7c8d322833e996815d06c18a07ddeecc3c83fe4f328df83f181` |
| `private.bpay_next_reconcile_work_collection_v1` | 4088–4397 | `1d000b50d3b90eb0547993d78de6ce03adb89258a4c7cd58035db6ea7616700b` |
| `private.bpay_next_work_collection_origin_v1` | 4403–4458 | `c990352456f536608b758e68e6a14a6763a63119fc89c22e790228d89ce672d8` |
| `private.bpay_next_work_collection_guard_v1` | 4464–4612 | `23c22083e1719e323d26f97c74be9c4e3a905e05bb387dc48edde8188db8639e` |
| `private.bpay_next_available_segment_recovery_origins_v1` | 4619–4637 | `754ab4bab71c47a172bd8e4b3a5412f1279372ef309fcf589fed24691510a348` |
| `private.bpay_next_segment_recovery_reservation_v1` | 4641–4680 | `7911ba0e30bf36d9de908574626bce6559a6d1e24621beccc378d1b7d027b399` |
| `private.bpay_next_segment_recovery_effect_v1` | 4688–4723 | `089a6a1af8504ef552dcdbaac4879398120310b4da9bc594bf08a49aaedfb95b` |
| `private.bpay_next_segment_write_off_membership_v1` | 4730–4764 | `ca3f51d570a0377bba468b544fab23efef96483a80d403b464a94dc1ff3af616` |
| `private.bpay_next_capture_segment_page_v1` | 2827–2951 | `d7e6329da788c023d822205b82f8ce7120732ee11ae5bcf558f20658d1432d65` |
| `private.bpay_next_work_collection_capture_guard_v1` | 3670–3807 | `4efa607ffa570b6cfbbeac53b6c128f9af2fcf31cf95d1636bb73cdd1a287e26` |


## Fresh negative fact / deferred membership supplement — 10 October 2026

Full read of `segment_negative_fact_guard`, `segment_membership_commit_guard`, their exact attachments, and the changed negative effect_share/effect_origin INSERT branches. No execution. **One actual frozen-recovery lifecycle finding (F3) remains; the guard arithmetic/types/owner and native timing checks below are otherwise sound in this bounded source review.**

- F1 closed: exact origin-leading indexes are now present: effect_origin `(origin_share_id,effect_share_id)`, recovery_share `(origin_share_id,case_hold_id)`, and write_off_share `(origin_share_id,case_event_id)`.
- F2 closed before installation: collection_basis now requires actual negative effect_share recovered gross equal native principal_recovered and exact W/H/K Case/component equality. A genuinely old unmapped recovery cannot become a partial `complete=true` basis merely because current WORK C has reached zero. No baseline or backfill was invented.
- Negative fact guard denies UPDATE/DELETE/TRUNCATE before NEW access. Exact origin/work/component/key, real collection scope, native DRAFT result/Case hold/FINANCE_ALLOCATE owner, captured observed revision and PAYE gross tuple are checked. Before each INSERT, aggregate amount already attached to the native hold/event is subtracted, and actual remaining origin capacity is checked. A row cannot exceed the whole real parent reservation or consume another key's debt.
- Forgiveness admission uses actual original requested write_off command, minimal WRITTEN_OFF event identity, Case/component expected revisions and native existing owner. It does not require fictitious recovery fields. Timing remains before the native forgiven-counter updates.
- Negative effect_share binds genuine RECOVERED event, REALISED Case hold/capacity, actual original outcome/member/instruction/result, active native settlement lease and Candidate/module epochs. Signed gross must equal exact retained recovery_share sum at the key. Negative effect_origin requires exact reserved origin and remaining realised original paid gross. These INSERT checks do not call the temporarily stale WORK current-basis helper before0316 reduces native C.
- Deferred constraint triggers reload actual current parent rows instead of the INSERT/UPDATE NEW status snapshot. They prove whole hold/share and effect/share/source-origin totals, native capacity status and amount, actual RECOVERED event/effect for a realised Case hold, native terminal write-off receipt, per-origin residual paid gross covering W+ACTIVE H, and whole Case/component recovered/forgiven/ACTIVE reservation totals. They issue no new owner lock or business command and require no current lease/module to validate immutable completed facts.
- Exact five fact relations get deferred INSERT constraints; native Case hold gets deferred INSERT/UPDATE lifecycle proof. New row/TRUNCATE immutable attachments are limited to two feature negative-fact tables. Existing money-owner bodies and frozen Source triggers are not replaced by these attachments.

### F3 — captured recovery is skipped after today's basis becomes unavailable

`segment_recovery_effect_v1` currently selects the live work_collection and returns before loading the actual event/hold whenever `segment_attribution_complete` is false. That mutable flag is current-approval qualification, not the immutable admission of an already allocated Case hold.

A concrete native sequence is: complete ordinary segment revision creates a real DRAFT recovery hold and retained recovery_share rows; a later approval uses an unsupported representation/current source and reconcile truthfully clears the current complete flag; the original accepted CSV/internal settlement later posts that real Case hold. Original0316 validates and honours the actual frozen instruction/hold/capacity/event without requiring today's origin helper. The new callback skips its retained negative membership. Native Case recovered counters then advance, and the new deferred Case membership assertion fails because the exact captured negative gross was omitted.

Small correction: resolve the actual RECOVERED event and real Case hold first, then decide whether this effect is segmented from the presence of retained `recovery_share` rows for that exact hold. No rows preserves the native unsegmented branch. Existing rows must be consumed completely using the current callback's genuine event/hold/capacity/amount checks, regardless of today's `segment_attribution_complete`/READY flag. The final guarded facts and reconciliation may report today's approval unavailable while preserving the original recovery. No latest origin guess, baseline, calculator, owner rewrite or second clock.

Raw repeatable snapshot `805ae8d0a68a1d4b7605505cd0e6322fae59d2dcbfe795fb9cbaebf7ada93d67`; raw migration `0a2e8a72f43bd6de767e0b277c8c895e02ed29482588a40c23cd51ebd8ac5ef9`. Exact definitions:

| Definition | Lines | SHA256 |
|---|---:|---|
| `private.bpay_next_segment_fact_guard_v1` | 3854–4035 | `1b23398cb12251a82f50cca70fa577614ae76fee9ffb511c8ca569dc8c028525` |
| `private.bpay_next_segment_negative_fact_guard_v1` | 4884–4948 | `98b85f8613cb128b9a23f6955a9167e73a9ed6c690f54a7d2dc63393fbe94726` |
| `private.bpay_next_segment_membership_commit_guard_v1` | 4965–5098 | `6bdcadc66875ea4654a606bd75002a38a2df07db087752da6e5335c6174de857` |
| `private.bpay_next_segment_collection_basis_v1` | 4078–4117 | `85abfe8de78b8d5787d9ec362981a1c88dc6274ed9506d59e3d0ad7befa85deb` |
| `private.bpay_next_segment_recovery_effect_v1` | 4722–4757 | `eda1533d66c67fd0fbb2cbf97634de030883933551594f1c09bc5915b90a5f36` |


## Fresh closure — negative INSERT and deferred lifecycle subset

**PASS, with no remaining concrete finding in this requested subset.** F1's three origin-leading indexes and F2's exact K plus W/H Case/component completeness checks remain saved. The negative fact guard and membership commit guard definition hashes are unchanged from their complete source reads recorded above.

F3 is closed in the freshest callback: after actual RECOVERED event lookup, `NOT EXISTS recovery_share(case_hold_id=actual event.operation_item_id)` alone determines its unsegmented early return. Any actual retained vector is consumed through the real REALISED Case hold/capacity/event and exact gross checks, regardless of mutable current approval qualification. The earlier mutable-only branch, and the interim `not complete AND no retained rows` form, are superseded by this simpler exact participation predicate. Today's complete/READY flag is not an admission requirement for honouring a frozen recovery.

Fresh write_off callback uses an optional lookup of the existing exact work-collection write_off_request and returns if absent, preserving other WRITTEN_OFF owners. When this actual request exists, its same-owner current-basis checks and genuine pre-counter event identity remain; it is a fresh forgiveness operation, so it has no frozen recovery replay needing that current flag removed.

The new guards preserve genuine timing: recovery_share insertion happens before capacity_use/counters; negative effect children happen after real Case realisation but before native WORK C movement; deferred reloads prove actual completed totals and lifecycle. A frozen old recovery can settle after current qualification is unavailable without source credit redistribution or an invented latest identity. Update/delete/TRUNCATE refusal precedes NEW access; feature attachments remain private and finite. No SQL/tests/runtime/business invocation was performed.

Raw current repeatable: `805ae8d0a68a1d4b7605505cd0e6322fae59d2dcbfe795fb9cbaebf7ada93d67`; current migration: `0a2e8a72f43bd6de767e0b277c8c895e02ed29482588a40c23cd51ebd8ac5ef9`. Exact reviewed definitions:

| Definition | Current lines | SHA256 |
|---|---:|---|
| `private.bpay_next_segment_negative_fact_guard_v1` | 4884–4948 | `98b85f8613cb128b9a23f6955a9167e73a9ed6c690f54a7d2dc63393fbe94726` |
| `private.bpay_next_segment_membership_commit_guard_v1` | 4965–5098 | `6bdcadc66875ea4654a606bd75002a38a2df07db087752da6e5335c6174de857` |
| `private.bpay_next_segment_collection_basis_v1` | 4078–4117 | `85abfe8de78b8d5787d9ec362981a1c88dc6274ed9506d59e3d0ad7befa85deb` |
| `private.bpay_next_segment_recovery_effect_v1` | 4722–4757 | `eda1533d66c67fd0fbb2cbf97634de030883933551594f1c09bc5915b90a5f36` |
| `private.bpay_next_segment_write_off_membership_v1` | 4764–4799 | `37d24318c11e23903a58595a30872ffbb351a2d04464c15a885c7e77975ec4d9` |

This closes only the requested negative-origin, private INSERT/immutability, negative effect branches and deferred native membership subset. Whole WB36 capture/selection/other guards, final registry/release controls and actual runtime receipt remain separate builder/other-reviewer work; none is implied by this PASS.
