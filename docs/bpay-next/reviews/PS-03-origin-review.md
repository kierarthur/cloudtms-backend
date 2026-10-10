# PS-03 member-origin SQL source review

10 October 2026. Independent read-only review of the full saved0554 migration/repeatable, exact PS-03 card/checklist and BD033 scope; compared current0145, simple0110/0533, reissue0240, Case0410, destination1827 and final-definer registry. No SQL, database, runtime, tests, fixtures, network, secrets or product changes.

**Initial verdict: one narrow correction required before SQL source PASS.** This verdict covers D01 enforcement and preserved D02 owner reuse, not the pending Worker/Office review or actual installed/app acceptance.

## PS03-SQL-01 — exclude Case NET requests from the ordinary branch

0554:47–51 admits `PAYE_MANUAL` projections and verifies the accepted request's worker/Candidate/entered NET/gross tuple, but does not require `v_request.case_draft_state_id IS NULL`. Original0390 legitimately creates `PAYE_MANUAL` requests for Case NET; original0380 explicitly distinguishes those by nonnull `case_draft_state_id`. A transfer retaining such a projection but lacking a Case/destination build can pass the ordinary NET branch with contribution `projection.cash_amount − projection.gross_inc_vat`. That difference incorporates Case allocations/recoveries into the NET member, whereas preserved0410 requires `entered_paye_net − gross_inc_vat` and separate typed CASE members under its exact bound state.

Add the finite `v_request.case_draft_state_id IS NOT NULL` refusal in the ordinary NET predicate, after the existing bound-owner delegation. This does not change a calculation or a producer: simple0110 already requires `case_draft_state_id IS NULL`; real Case/destination transfers have already delegated to0410/1827. No current worker revision, historical aggregate or new identity is needed.

## Boundaries that pass source review

- The migration adds only one partial UNIQUE `(transfer_id,subject_kind)` for NET_ADJUSTMENT/CASH_REISSUE. Existing ordinary and Case/destination builders emit at most one of each kind per transfer; destination group uniqueness remains intact. Existing WORK/CASE uniqueness and disjoint literal-origin checks remain authoritative. No backfill or fabricated line/hold origin is introduced; pre-existing duplicate singleton rows would fail installation rather than be silently rewritten.
- The replacement keeps original0145 non-INSERT refusal first, takes the same exact transfer `FOR SHARE`, refuses non-BUILDING, and binds NEW worker to that transfer before delegating. Case/destination exemption therefore cannot reopen UPDATE/DELETE or append after completion. Original0410 still performs exact stages/worker/result/hold validation, and1827 still records/routs destination members; zero Case result with no hold remains valid. The new guard does not change their formulas.
- Ordinary WORK requires a real same-worker frozen line, exact signed frozen INC and an original projected transfer. Core same-worker composite FKs and existing WORK uniqueness remain in force. Simple0533 BUILD and CLAIM0130 are unchanged; this is a supporting0145 guard successor, not an0130 editor-chain advance.
- Ordinary NET uses the transfer's exact retained projection and that projection's actual request command, with null-safe tuple/value comparison. Its current simple0110 accepted request/projection gross values equal the frozen gross used by unchanged0533. No fresh worker settings or revised projection are selected. The Case-request exclusion above is the sole remaining boundary defect found.
- CASH_REISSUE resolves the actual cash obligation, original transfer and request by `transfer.build_command_id`, checks exact worker/Candidate/original/request tuple, requires member1 and exact leg cash, and allows the request's transfer_id to be NULL or this exact transfer. This respects0240's real order: transfer INSERT → member INSERT → MEMBERS_READY → cash reservation → request transfer attachment. It correctly does not require the reservation/attachment to exist before the member.
- Core required fields, disjoint CHECKs and FKs supply the remaining null refusal; `IS DISTINCT FROM` protects the new equality comparisons. The guard preserves original SECURITY DEFINER/fixed search_path/logical postgres owner and revokes PUBLIC/anon/authenticated/service_role. Existing attachment remains, with no new public trigger or grant.
- Registry changes only `private.bpay_next_transfer_member_guard_v1()` from0145 to0554. CLAIM remains0130, BUILD remains0533, Case0410 and destination1827 remain their current finals. The SQL history reader and its later PS-07 chain are unchanged under the final BD033 graph approach.

## Reviewed raw SHA-256

| Saved source | SHA-256 |
| --- | --- |
| migration0554 | `b6a3cb6c1f681926e97136e00f3a4c99b44a5da0f477c87720592d5fda0d4120` |
| repeatable0554 initial candidate | `798f1ce6700d05747041119466d87dd6e8665bcf465796563aa29ca88fbf87af` |
| final-definer registry | `667162a35b4c7410a781e16445c00c58a787fbff7d156ea5340ecb77c6fa6c92` |
| BD033 scope | `c654440738e4d93563fb231ee05423d5e6ae46f126f1094a81fc1df0cce9e76a` |
| original0145 | `2006007829398c9d849ddad959ea7e665f07b37a49f08dddd3e56385c8623a2c` |
| simple0110 | `d74d8a9629ef245eb8940c4026e0ebd2b99d9d81a72b96708c72673164669c1e` |
| final BUILD0533 | `a7175c21cac86b372ad59c3a114dce69c9298708154898955848877b020fb314` |
| reissue0240 | `5f8365702daf773fae696a4c0861892f5fd19be701c517a00bbbbee07e50e714` |
| Case0410 | `f56898dc7e992fc271d2a90c0c7fdb85d77dde4bac5e8b852e008c0da90793d8` |

Actual business, installed, app, Worker/queue/minute acceptance remains NOT_RUN. D06/D07 typed-history implementation is a separate pending review; no whole-card completion is claimed here.

## Fresh corrected SQL closure — PASS

Reread the full saved corrected0554 repeatable, unchanged migration and full PS-03-JOINED-ACCEPTANCE.md. PS03-SQL-01 is closed: the ordinary NET predicate now explicitly refuses nonnull `v_request.case_draft_state_id` after exact Case/destination delegation. This preserves the actual simple0110 producer and the distinct bound0410/1827 formula without selecting mutable settings.

The supporting RECORD conversion is sound by source inspection: every record field subsequently accessed is selected explicitly from its exact native-key row, without a whole-record projection or duplicated output name; missing optional origin rows raise before their fields are used. The retained destination helper still returns its declared existing composite. No owner, trigger, money formula, cursor, request attachment ordering, ACL or other defining routine changed.

Final SQL source verdict: **PASS for the reviewed D01 enforcement/D02 reuse subset**, with no open concrete SQL findings. The saved joined checklist truthfully keeps actual cases NOT_RUN, distinguishes simple/Case NET formulas, permits legitimate zero CASE and reissue pre-attachment, and preserves server conservation/no partial-page UI sum. Worker/Office D06/D07 review remains separate.

| Final saved source | SHA-256 |
| --- | --- |
| corrected repeatable0554 | `d16afe22d2ec650335843e4b001cb81dd69fca443dff17872d531e8c036dc4e4` |
| unchanged migration0554 | `b6a3cb6c1f681926e97136e00f3a4c99b44a5da0f477c87720592d5fda0d4120` |
| joined acceptance | `05c98eb5cc3ea96f1772ffff841bad1c70737555bd644858375be2d707c3efcb` |
| unchanged registry | `667162a35b4c7410a781e16445c00c58a787fbff7d156ea5340ecb77c6fa6c92` |

No reviewer execution or product edits were performed; source PASS does not establish installation or actual joined results.
