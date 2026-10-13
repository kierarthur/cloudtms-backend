# WB-29 W2 command — independent source review

12 October 2026. **Initial checkpoint: OPEN, concrete finding below.** Root alone writes product. This reviewer is reading source/native schema/design and writing this receipt only; no SQL/RPC, tests, compilation, fixtures, business data, hosted action or product edit.

Initial reviewed source: `supabase/repeatable/12102026_2320_banking_pay_next_component_resolution_v1.sql`, SHA-256 `6f92ab42f4a5fb19778130c9eed2a5c804e2947f2a3f2a54dbe5a9745eedd857`. This initially contains the private transaction only; public wrapper/closed transport and complete final source remain pending review. Whole WB-29 is not approved.

## W2-S01 — missing exact authenticated actor prelock

The private owner checks active/admin actor with an unlocked EXISTS (29–31), then the public qualification/prelock block (135–143) covers Candidate, Timesheets, financial snapshots, Contracts and Umbrellas but omits `public.tms_users`. The W1/context helper likewise reads actor eligibility rather than holding it. Native decision/event/receipt actor foreign-key checks can consequently acquire their first actor row lock after publisher1 has obtained the agency command clock; the initial actor qualification also can change after its read. The saved `WB-29-w2-publication-prelocks.md` explicitly requires the actual actor qualification and FK scope before any seed/publication/global clock.

Smallest correction: prehold this exact authenticated `public.tms_users.id` with active/admin qualification using `FOR SHARE NOWAIT` before first retention mark/seed/publisher, and fail closed if absent. Preserve receipt-first replay and readback; do not make historical replay depend on today's actor metadata beyond the existing authenticated admission contract. No new actor table, proof or protocol is needed. Reported to root immediately.

## Checks completed at this checkpoint

The transaction retains original request equality and receipt-first replay/readback before fresh epoch/date/basis; acquires NEXT module then Candidate control and the native financial fence; preholds sorted full explicit Work/Position/related-Case scopes; enumerates all mapped ACTIVE target/VAT/policy/date variants before compatibility filtering; stages every full Work before publisher1; uses actual publication child IDs; writes CLEARED while old ACTIVE then clears state; retains unrequested siblings through the exact2313 carry provenance; and places the immutable W2 receipt last. Source seed plans precede the actual seed and READY recheck; READY is not reseeded. These observations are not final approval: detailed native guard/event/quote/result-bound and public-wrapper checks continue.

## Final source closure — 12 October 2026

**SOURCE PASS at SHA-256 `5b51688cb62ff51cfabdd1a67c95d5d526586a6425fdd8387dc9e9234669cca8`.** This supersedes the initial OPEN checkpoint. W2-S01 is closed: current private lines136–137 explicitly prehold the active/admin actor with `FOR SHARE NOWAIT` and refuse absence. The completed public wrapper also retains its original active/admin actor SHARE admission before private Candidate locking. No further blocking source defect was found in the complete private owner plus public wrapper reviewed here.

### Replay, closed context and native owners

The original six-key args and shared scope validator retain the native 32KiB request, one Candidate, distinct <=100 Works and cumulative <=100 requested rows. Raw manual text is validated before numeric coercion; CLEAR requires an exact non-NULL pair and distinguishes owner IDs, while SAVE rejects duplicate financial identities. The authenticated original immutable receipt comparison precedes fresh module epoch, metadata, date, Position and decision interpretation. Readback with no receipt remains a definite absent receipt, not fresh dispatch; a second original-receipt probe follows Candidate serialization.

The supplied context must equal exactly the current native fourteen-scalar offer context. The large observational `context.works` is not accepted as browser financial authority. Current expected revision/current/applied/no, physical Timesheet/version/booking/Contract, native Position A/R/H, immutable policy/date/current target and exact expected decision pair are independently qualified under the complete native locks. The native W1 reader is used for actual scope/physical/availability qualification without any seed; actual intent quote is obtained directly from the unchanged2218 shared input quote. No browser Source, units, financial totals or inferred rates are used. The currently compatible owner alone is replaced on SAVE. Exact incompatible CLEAR remains independent of today's target/date.

### Lock order and complete preflight

Module→Candidate control→existing Candidate financial fence is retained. All explicit Works lock in UUID order, current/original approval roots are natively proved and SHARE preheld, and the2316 complete physical revision census runs before mutation. Full physical Position/collection membership has its own cumulative10000-row/8MiB sentinel census; all Positions then lock by Work/component, complete related Cases by UUID with same-Candidate proof, and existing exact ORIGIN tasks by key before metadata. Related Case count>100 is refused rather than pretending a prefix is complete. Existing refresh_many's combined >100 continuation behavior is retained.

The bounded mapped closure includes both current based-on and published boundaries before target compatibility filtering. Its actual full target/Umbrella/VAT/policy/date identity is deduplicated, current mapped native basis checked, and cumulative100-member/64KiB scope refused on overflow. Old targets/Umbrellas/policies and all current/root physical/snapshot/Contract identities are preheld before seed/publication; stronger public qualification uses NOWAIT, avoiding a new wait on a public trigger under Candidate ownership. Actor qualification is covered as noted above. Actual public retention marking for the complete physical set precedes the first Source seed/global refresh. No unsubmitted Candidate/Work or history inventory is discovered.

Current lines206–234 now plan the entire closed output before retention/seed/stage. UUID-shaped placeholders and maximum bigint widths are byte-budget operands only, not persisted identities. Every requested result plus every retained old→new pair is included; an oversized scope refuses before publication. Final actual output/result bounds are checked again. There is no truncation or silent sibling omission.

### Source authority, complete stage and event order

Every required SOURCE component from requested rows and mapped siblings has a2316 plan in sorted native scope before the first seed. Its original1246 witness proof plus full maintained R/H equalities is reused. READY returns no seed and retains the historical seed tuple; UNBOUND is initialized only through1309, followed immediately by current2146 READY and exact Candidate/canonical Work/component verification. Signed/ambiguous/unproved native book refusal is preserved. No reconcile, first-effect-as-money, history sum or reinitialization is added.

Every submitted Work is fully staged through2135 before publisher1 acquires its native POSITION_APPLY clock. The unchanged publisher owns seal, complete root payload proof, pointer/Processing change, original child command/member and publication. Work Position remains the preheld predecessor. CLEARED uses the old decision's based-on line while old state is ACTIVE, then only state changes. Explicit clears/replacements are omitted from carry; every remaining structurally mapped variant carries unchanged native selected mode/manual operands/target/Umbrella/VAT/policy/date/reason/original Position scope with exact old pair/request provenance. The2313 guard positively joins the actual child, predecessor/mapping, old CLEARED event and complete new publication proof. Fresh requested SAVE keeps current Position admission. All new ACTIVE versions receive their own SAVED event. Immutable captured references remain old IDs/versions; no existing offer is repriced.

Native targeted refresh and truthful Processing/adoption follow the complete mapping, and the immutable W2 receipt is inserted last. Any later native guard/structural failure rolls the whole transaction back; no exception handler manufactures partial acceptance.

### Public wrapper and source-gate boundary

Static inverse comparison removed only the two new action literals and the exact early private delegation branch from `public.bpay_next_workbench_command_v1(jsonb)`. The resulting normalized CREATE statement equals the complete0312 parent byte-for-byte after line-ending normalization: **4408 characters**, successor4662. Existing service/admin admission, Snooze/many/readback/retry behavior, original4KiB Retry limit, wrappers and ACL remain unchanged. No new route or Worker kind is added by this SQL batch.

Current source-gate additions are finite signature/callee edges for the three named W2 helpers and the existing public wrapper's private delegation. The added edges name the actual census/snapshot/seed/stage/publication/quote/refresh and static JSON constructors. They do not authorize dynamic SQL or broaden unrelated parents. This reviewer read the source; it did not execute gates, compile or inspect an installed catalogue. Root's normal checks remain separate evidence.

| Current evidence | SHA-256 |
|---|---|
| Full2320 private/public source | `5b51688cb62ff51cfabdd1a67c95d5d526586a6425fdd8387dc9e9234669cca8` |
| W2 native command design | `dd9e0a5bfcd187c4b487da24cb2354b3911ad4bff2cb8a18e15c41df5a207744` |
| BD161 inclusive carry | `176e92503e55a99c813a504e27b22423adfe888e849bf4ee23d8568a76a4ad51` |
| Context/native request design | `588907556dbbd4faa7e36255155b23f8e738b8e0f5237f8e8d1e8b184806890f` |
| Source gates at this inspection | `bc373126d68f0b3640ddc7ec253805a0e27136532d4b5b2dc79ec0d3aa490572` |
| Registry at this inspection | `74a0e309eab16a4a0693c4bdd3af2325bc35c835ef7b937527a7de96b086cf1e` |

The reviewer authored the earlier exact carry-guard design and has disclosed that relationship; this is an independent review of root-authored product bytes, not a second independent approval of its own design. W2 Worker/Office transport, captured composer/hold/Ready/frozen joins and remaining whole WB-29 deliverables are not certified by this SQL receipt. Whole ITEM remains open; joined/business acceptance is NOT_RUN.

## Final current-byte amendment

**SOURCE PASS at full2320 SHA-256 `1dd72479ba41c1997439f80b8a2546d285f29cd1f791bbdf28dc2fc615385ce2`.** This supersedes the preceding source hash; W2-S01 remains closed. Fresh source adds only the checked native Case namespace prerequisite and exact Contract Client FK prehold to the complete transaction reviewed above. Related Cases must be the original same-Candidate collection Case/component identity for one of the already-censused explicit Works; this agrees with the retained native collection identity guard. Contract Client IDs are deduplicated and KEY SHARE NOWAIT preheld before retention/seed/publication. No new Client financial authority, collection formula, publication owner or broader Work discovery is introduced. The corrected DISTINCT Source planning subquery preserves the same sorted complete requested-plus-carried component set and the pre-mutation result budget remains intact.

The public wrapper inverse was rechecked against0312 at these current bytes: removing exactly the two action enum additions and early private delegation still reconstructs the complete normalized parent statement (4408 characters). Original replay, admission, Snooze and Retry behavior is retained.

Current source gate SHA-256 is `4047f588297aece326c9259b6b8e008d452becfac000e2ff97ec70c5db71a617`. The new history allowances contain only the exact seed-plan point/prefix statements; unrelated history refusal remains. The UUID singleton allowance additionally requires the exact2316 physical path, exact seed-plan signature, exact comment-free executable body SHA `8b80ec84be009ba612ac923e6abd2c6e29c116df8b27894cfdf93c158f0967ca`, and the literal MIN argument `X . ID :: TEXT`. Static extraction using the gate's normalized comment-free body convention reproduces that SHA. The pinned body contains exactly two such aggregates; each obtains count and UUID together and refuses count<>1 before that UUID reaches the unchanged1246 proof. Direct UUID aggregates and every unpinned text-to-UUID aggregate retain their original hazards. This is finite evidence for the installed native singleton queries, not an unrestricted aggregate or dynamic SQL allowance.

Registry remains `74a0e309eab16a4a0693c4bdd3af2325bc35c835ef7b937527a7de96b086cf1e`. This amendment is source-only; this reviewer performed no executable gate, SQL, catalogue, test or joined action. Whole WB-29 remains open.
