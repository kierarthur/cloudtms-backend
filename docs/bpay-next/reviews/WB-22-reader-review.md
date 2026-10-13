# WB-22 independent reader source review

Verdict: PASS for the reviewed finite SQL reader/count/export-memory scope and read-only helper source-gate closure. RR01–RR07 closed on current2306+2340. Worker/FE caller implementation and joined app journeys are separate; no whole-item/runtime claim.

## RR01 — whole inventory is not a bounded row authority (open, root correcting)

Leaves11–24 aggregate every eligible native segment into one component JSON array. Migration14 then caps that one basis at32KiB; row739–775 aggregates all component bases again for Work/Case parent and returns this as selection_basis with another32KiB refusal. A legitimate Work/component with enough native members, or a Case with enough components, cannot be read/captured even though original native lists can page those members. Candidate later overwrites the array with one scalar but still computes it first. The reply no longer has a fixed bounded tuple, and a byte refusal does not make inventory transport finite for valid large native membership.

Root's accepted correction is finite per-native LEAF basis rows with exact component set equality in the server, plus a fixed existing local authority/result tuple in the reply. No new digest/clock/producer, projection owner or monetary authority. Segment inventory stays server-side and component intent remains the full original component unit. A native segment/member addition/removal must participate in set comparison both directions. Native pagination and full equality cannot be replaced by a first100 inventory cap.

## RR02 — selected_member_count changed from native members to components (open on reviewed snapshot)

Common HEADER943–946 counts choices rows. Initial leaves37 groups all WORK_SEGMENT members of one approved_line/component together, whereas original1332 contribution472/478 and common2128 HEADER151–159 count selectable PAYMENT/DEDUCTION native rows. A Work with two eligible segments in one component contributes two existing native payment items but this new branch returns one selected_member_count. Preserve the established named count semantics by counting selected native eligible members of the selected component, with the same exact payable-Candidate/Case-parent qualification as Create. A new separately named component choice count is fine; silently substituting it for native selected_member_count is not. The per-native LEAF correction may close this naturally; verify the final SQL after composition.

## RR03 — HEADER does not return required bounded deliberate intent (open on reviewed snapshot)

Common HEADER949–950 adds selection_memory and selection_mode only. BD077 explicitly requires HEADER's own bounded acknowledged raw four-kind intent so clean reopen can restore hidden deliberate choices as well as leaf defaults. Returning only effective booleans loses exact hidden overlay information retained by capture112–130. Add the stated fixed shape/32KiB bounded raw intent from the exact acknowledged HEADER, or an equally explicit existing owner seam consumed by Office. Do not expose all LEAF rows, synthesize intent by enumerating defaults, or retag the current journal.

## RR04 — count overlays do not prove native tuple identity (open on reviewed snapshot)

Intent validator709–734 checks shapes/UUIDs/duplicates. Choices98–101 joins a Work by work_id only, component by approved_line_id only and Case by component ID only; it does not compare the supplied Candidate or Work expected_revision_id to the actual scoped native tuple. An overlay can name the real Work under a different Candidate or an older expected revision and produce selected HEADER counts, while original Create rejects that same tuple. Count/default local reconciliation must fail closed for a stale/mismatched explicit native tuple rather than count it as current. Reuse the exact scoped native identities already returned by leaves; no signature/security/economic changes required. Cross-kind component/Work SUBSET constraints must stay aligned with the original accepted descriptor validator.

## Findings already closed in fresh reviewed snapshot

- **Original SUBSET fallback CLOSED:** choices105 now selects only explicit positive component overrides for a fresh selected Work SUBSET; unlisted components default false. The previous branch silently fell through Candidate/ALL defaults. Current Candidate false/Work false precedence remains.
- **Candidate-narrowed HEADER scope CLOSED:** HEADER943 now calls choices with actual `p_scope`, not the broader original `v_memory.scope`. This preserves optional narrowed Candidate counts under a broader acknowledged memory namespace. Row744 similarly narrows Candidate before native choices. Keep the baseline namespace binding broad only as its exact reference, not the current count universe.
- **Inert GROUP_LINES CLOSED:** row753–754 returns unselectable/defaultfalse/no-basis for rows with no Work and no Case component, avoiding an unrelated Candidate-wide rollup.

## Preserved source boundaries assessed

Public successor keeps original service/admin/NEXT/agency/current-date/read-snapshot checks, original projection-not-CURRENT refusal and HEADER job_progress. It resolves actual full namespace via memory_resolve, validates exact acknowledged run/scope, distinguishes explicit detachednull, and appends actual resolved memory to cursor context before the original cursor equality. Exact kind keys and original sort/seed/page receipt remain. Common diff is bounded to HEADER memory/count and post-seed row enrichment; all current export/issue/detail/navigation templates are preserved. Original ten kinds, limit<=100 (groups<=10), row110KB/response120KB, request32KiB and5s timeout remain. No new public RPC or financial side effect is introduced by these reader changes.

All helpers are private security-invoker; exact logical postgres owner/private ACL and service-only original public reader tail are retained. Actual supported fields referenced by Case basis (component_revision/rule_revision/threshold/floor/weekly_due/next_due_monday) exist in original0300 schema. Worker/FE closed mirrors and caller generation checks are pending root composition, not reviewed or waived here.

Next finite review: bind to root's amended per-native basis/current SQL hash, check RR01–RR04 closures and exact fixed local authority/NULL shape; then inspect closed mirrors. No runtime/app acceptance claim.

## Fresh reader supplement — reviewed3208813de8488b232f5cd0c5fc0d8255e268c1f2727d7ba0cb98206aacdd45ec

- **RR01 CLOSED:** leaves now emit one fixed native member basis each. Choices84–128 performs component unit/set equality server-side, including both current-to-prior and prior-to-current membership. Row775–819 no longer aggregates/returns inventories; it returns fixed native reconciliation operands. Child basis has no Candidate FV/timestamp; Work parent authority refinement is separately RR05. Genuine segment/component intents remain original.
- **RR02 CLOSED:** HEADER989–994 selects component units then counts their genuine current eligible Workbench members by native approved-line/Case-component identity. This retains native selected_member_count rather than counting each component once. It applies actual p_scope and payable Candidate qualification.
- **RR03 CLOSED:** HEADER986/997–998 now returns exact acknowledged `remembered_intent` plus memory/mode. Its original capture is bounded raw typed intent; leaves are not transported.
- **RR04 remains:** choices117–120 still matches raw Work/component/Case overrides by IDs without their supplied Candidate/expected revision. Exact stale/mismatched count validation must be checked in final source.
- **RR05 (root correcting):** row783 currently returns coarse Candidate FV for every CANDIDATE_GROUPS row. That causes a Work/ROW/Case child local untick to stale from an unrelated Work of the Candidate. Restrict coarse marker to Candidate parent; use fixed actual child authority. Root requested/reviewed own Work position timestamp/count/direct aggregates as local-only guard. The dedicated note `docs/bpay-next/design/WB-22-local-authority-tuple.md` contains exact eight amount-update timestamp statements and Source seed1309 exception (book revision covers it), plus Case native counters. Case row787–798 now includes its actual mutable monetary/resolution counters, closing the earlier component_revision-only omission.

The local Work timestamp may be used as existing mutation evidence with its direct native fields, never persisted New/Changed equality or an exact component-vector digest. No new version owner is required. Concrete source/mirror final review and joined app acceptance remain separate. Historical diagnoses above stay preserved as provenance; this supplement supersedes their open labels for RR01–RR03.

## Fresh final-draft2306 review, 10 October 2026

Read-only source review; no SQL, tests, runtime, business rows, app or network execution. This supplements and supersedes the earlier open RR04/RR05 findings at the reviewed snapshot. The parent's local PG17 compilation is separate evidence; this review does not claim an independent compile or application result.

| Source | Raw SHA-256 |
|---|---|
| `supabase/repeatable/10102026_2306_banking_pay_next_actor_selection_memory_v1.sql` | `3f77bbb57500f1320d3873935c8504264a46e50287be9740267f838f8d1dc750` |
| `supabase/migrations/10102026_2306_banking_pay_next_actor_selection_memory.sql` | `51cdfb58b668afb18bfa83083d2a14ac3c33b701a6d635feb2b4e592db8c49bc` |
| `docs/bpay-next/design/WB-22-selection-memory.md` | `42d250250f540508c8c27e3d2e265dd1609781e0fbb9f722b934b9b70dbeea77` |

RR04 is closed for the actual native Work/Candidate/current-and-applied revision/approved-line and Case-component tuples: public owner lines1681–1691 now refuses a stale or mismatched tuple before invoking the count branch. The exact acknowledged baseline must match the request's count reference at1679–1680. Old simple count requests remain supported. RR05 is closed for the Work parent:789–801 uses its own current/applied revision, position count/source balances/max financial-row mutation timestamp/Source book revision and applicable controls/settings. It no longer substitutes Candidate-global financial_view_revision for a Work. Component815–828 and Case802–814 use their own finite native operands; Candidate-parent786–788 alone retains Candidate authority. These fixed local reconciliation tuples are bounded display/copy evidence, not the persisted exact baseline or a new version authority.

RR01–RR03 remain closed: one finite native member per persisted LEAF and server-side complete component set comparison replace the old accumulated inventory reply; native HEADER selected-member counts join actual eligible item rows at1005–1011; bounded original acknowledged raw intent is separately returned at1002–1014. Explicit SUBSET selects only its true raw component IDs at127. Inert GROUP_LINES returns false/null at775–776. Full namespace resolution46–81 is agency/epoch/actor/official-date/original normalized scope, with Candidate-only narrowing and explicit detached-null semantics. The resolved generation enters the actual cursor context1692–1698, so a cursor cannot silently move to another acknowledgement.

**RR06 — remaining finite count-preview/Create parity.** The new public identity validation accepts two request shapes that the original acceptance owner rejects:

- An otherwise valid component override with no exact matching Work/Candidate/expected-revision parent tuple. `selection_memory_intent_valid_v1`731–759 validates finite shape/UUIDs/duplicates but not that parent relation; public1682–1689 checks native identity only. `selection_memory_choices_v1`122/128 can therefore count an orphan component. **Current Create2306:373–378 requires the exact matching Work tuple but explicitly allows hidden Work false/ALL when `v_memory` is true.** The extended count variant belongs to that memory-aware path, so mirror exact matching identity only; do not impose the old1832 unconditional selected-SUBSET condition. No reader kind, new authority or projection rewrite is needed.
- A Candidate override UUID absent from actual `public.candidates`. The reader shape validator accepts it and the choice join120 silently has no matching current leaf. Original Create1832:218–228 includes raw Candidate identities in its parent census and rejects one not actually held/existing. A plain native Candidate existence check gives the corresponding finite preview refusal; it must not add a new scoped-Ready-membership rule because the original owner deliberately retains valid hidden overrides.

Export memory enrichment is structurally sound at967–973: each original bounded native EXPORT_ROWS seed includes actual Candidate/Work/revision/approved-line or Case-component identity, then invokes the same GROUP_LINES memory helper; neither exported monetary operands nor native order/keyset are changed. The six additional memory fields remain fixed/finite, with the existing110KB row-array and120KB envelope fences retained. Callers must continue using the original `choice_eligible` and Candidate payable facts for Included; memory defaults do not turn a WAITING/RESERVED/EVIDENCE row into selectable financial membership. Worker/FE mirrors and CSV use are a separate pending caller review, not certified here.

The saved BD077 implementation refinement is consistent with the reviewed narrow scope: precise persisted equality excludes clocks, Candidate-global FV, names, warning and accepted presentation; local Work mutation timestamp is only reconciliation evidence and the native Source-seed exception is covered by book revision. Originally unobserved or ambiguous dirty intent remains visibly unresolved. The native pay-hold lookup19–21 now reads current actual Timesheet financials as original Create does. No projection producer, money owner, F25 parked object, protocol, digest or new clock was introduced by these reader closures. Application300-row/revision/NONE/Keep/next-date journeys remain NOT_RUN.

### RR06 current-owner correction

The initial RR06 recommendation incorrectly used old1832's unconditional selected-SUBSET relation. Fresh re-read of the same final2306 hash confirms the current memory-aware Create condition373–378: exact Work/Candidate/expected-revision parent is required, while `v_memory` permits retained hidden ALL/false Work intent. The remaining finding is the missing exact parent tuple, together with real Candidate existence; a new count successor should preserve those hidden choices. The old five-field descriptor path continues using its original selected-SUBSET requirement. This correction changes the proposed finite fix, not the acknowledged/default selection authority or the two mismatches themselves.

## Current2340 successor and gate review

Read fresh saved2340 full public definition and registry. Exactly one definition is present. Its body is the complete2306 public W1 owner with only the finite real-Candidate/exact-Work-parent validation block added; original scope, cursor, ten-kind dispatch, progress refusal, statement timeout and response byte fences are unchanged. Owner/grants remain postgres and service-only. Registry finalRoot/finalPhysical/finalClosures point2340 with2306 retained as predecessor.

| Source | Raw SHA-256 |
|---|---|
| `supabase/repeatable/10102026_2340_banking_pay_next_selection_count_acceptance_v1.sql` | `aabfb24a7abfef90a79d7f0c0bc7b42c2231ad5c786267f4104683d3d14239d7` |
| `docs/bpay-next/final-definer-closures.json` | `48f20942201c6bcf0b395d0aecc781db2f03eb6b5c4de6ff07e5866c1449c1b6` |
| `scripts/bpay-next-source-gates.mjs` | `1b3dc76a82e1aa139af81cb5cdb03ff81640d09102376cf59fb81774353f7555` |

RR06 selection semantics are closed:2340:143–149 verifies actual public Candidate identity and exact component-parent Work/Candidate/revision. It deliberately does not require selected SUBSET, preserving current memory-aware Create2306:373–378 and hidden ALL/false Work intent.

**RR07 — permanent invalid input becomes retryable unavailable.** The new block raises23514/BPAY_NEXT_CHOICE_NATIVE_IDENTITY_INVALID at149, but the unchanged public catch253–259 does not recognize this SQLSTATE/message. It therefore returns BPAY_NEXT_WORKBENCH_UNAVAILABLE with retryable=true for an overlay known permanently invalid. Smallest fix: raise22023 for this new reader validation, reusing the existing BPAY_NEXT_WORKBENCH_REQUEST_INVALID mapping and retryable=false. No new error enum/decoder or retry protocol is needed. If2340 has become installed, use a new dated successor.

### Finite routine-history helper closure

At reviewed source-gate snapshot59d4a9fa…, only public W1's approved edges gained intent_valid/resolve/unnest. `assertRoutineHistoryPoints` does not recursively inspect an approved callee (633–653); the outer signature loop1181–1188 inspects only `ROUTINE_WORK_SIGNATURES`. Therefore the five new read-only helper bodies should be listed explicitly there, preserving the existing finite policy rather than accepting arbitrary private delegation. Exact necessary signatures and allowances:

| Signature | Exact non-native callee allowances |
|---|---|
| `private.bpay_next_selection_memory_intent_valid_v1(jsonb)` | `octet_length` |
| `private.bpay_next_selection_memory_resolve_v1(jsonb,jsonb)` | none |
| `private.bpay_next_selection_memory_leaves_v1(jsonb)` | none |
| `private.bpay_next_selection_memory_choices_v1(jsonb,uuid,jsonb)` | `private.bpay_next_selection_memory_leaves_v1`, `bool_or`, `lateral` |
| `private.bpay_next_selection_memory_row_v1(text,jsonb,jsonb,uuid)` | `private.bpay_next_selection_memory_choices_v1`, `jsonb_set`, `split_part`, `max`, `sum`, `filter`, `octet_length` |

Native JSON/count/exists/boolean calls already have finite scanner allowances. The actual helper graph is public W1→intent_valid/resolve; maintained common kind-page→choices for HEADER and row for the three lists plus EXPORT_ROWS; row→choices→leaves. Capture/Create/seal write owners are outside this read-only helper closure and must not be added under a blanket memory-file waiver. Common kind-page already has its separate closed dynamic-template gate; do not insert it into the no-EXECUTE routine loop or weaken that assertion. No code or tests were executed/edited by this reviewer.

## Final installed2340 source closure

Fresh source-only re-review of the installed successor; no SQL/test/runtime/app execution by this reviewer. Earlier2340 draft hashes and open RR07 are superseded by the snapshot below.

| Source | Raw SHA-256 |
|---|---|
| `supabase/repeatable/10102026_2340_banking_pay_next_selection_count_acceptance_v1.sql` | `545ab73faec8f1331f33986b91ee743340f96be7bcecfb36b8a9c4ed12b38b14` |
| `supabase/repeatable/10102026_2306_banking_pay_next_actor_selection_memory_v1.sql` | `3f77bbb57500f1320d3873935c8504264a46e50287be9740267f838f8d1dc750` |
| `supabase/migrations/10102026_2306_banking_pay_next_actor_selection_memory.sql` | `51cdfb58b668afb18bfa83083d2a14ac3c33b701a6d635feb2b4e592db8c49bc` |
| `docs/bpay-next/final-definer-closures.json` | `05fd184ae3f7df55e77a01f04eb355287eb396a98e35ef5810302dff072005a1` |
| `scripts/bpay-next-source-gates.mjs` | `cf3069338e3326788bc926d86ff4fdc15b8c98a5480f72b8fba70e13df97d70b` |
| `docs/bpay-next/design/WB-22-selection-memory.md` | `42d250250f540508c8c27e3d2e265dd1609781e0fbb9f722b934b9b70dbeea77` |

**PASS, finite SQL reader scope.** Current2340 contains exactly the two registered complete definitions: public W1 and private common kind-page. Public is full2306 with only the exact real-Candidate/component-parent checks; private common is full2306 with only the EXPORT_ROWS Candidate default-pay boolean. Both registry physical/root pointers now2340; public remains service-only and private common has all browser/service grants revoked. Existing ten kinds, static sort/typed keysets, bounded native seed, progress failure branch, five-second public timeout and row/envelope fences remain intact.

RR07 is closed: new validation raises22023, which the unchanged catch maps to BPAY_NEXT_WORKBENCH_REQUEST_INVALID. It is absent from the retryable pair, so the permanently invalid overlay is not turned into a retryable Unavailable result. Cursor-stale still retains its original reload-required behavior. RR06 remains closed under the actual current memory-aware Create condition: exact Work/Candidate/revision parent required; hidden ALL/false Work intent remains legal.

The new EXPORT_ROWS `candidate_default_pay_selected` is a factual EXISTS over this same row's Candidate and original Client/channel scope, using the resolved acknowledged baseline and existing choices helper, restricted to an actually selected eligible PAYMENT unit. It does not aggregate money, fetch a browser inventory, manufacture an intent or override original native `choice_eligible`. Deductions-only Candidate therefore cannot obtain a positive default-pay qualifier merely from deduction units. Original19-column source mapping, Candidate/group/native item sort and monetary fields are unchanged. Future caller use must combine the same default fact with original eligibility and fresh explicit local intent under the existing exact descriptor; this SQL review does not certify a still-evolving CSV caller.

The source gate now includes exactly the five read-only helper signatures and allowances listed above. It resolves their actual final definitions and runs the unchanged no-history/no-EXECUTE/closed-delegation assertion over each, closing the previous nonrecursive approved-callee gap. No capture/Create/seal writer or full-file waiver was added. Common's dynamic-template gate remains separate.

Aggregate type classification is confined to `private.bpay_next_selection_memory_row_v1(text,jsonb,jsonb,uuid)`. It reads actual1324 Position and1100 Snooze DDL, verifies `updated_at_utc timestamptz not null` and `control_sequence bigint not null`, and requires the actual literal POS/SN relation aliases used by that fixed helper before supplying those two column types to the normal scanner. This is schema-backed type evidence for its existing max timestamp/control operands, not a numeric-array/order hazard waiver. All other unresolved aggregates still take the original refusal path.

No remaining concrete SQL reader/count/export-memory or finite helper-gate finding in this snapshot. Precise persisted per-native equality and bounded local reconciliation retain the reviewed BD077 distinction. Parent-reported nine source gates and solePG17 compile are separate execution receipts. Worker/FE/store/mirror completion and the300-row/revision/NONE/Keep/next-date joined app journeys remain outside this source verdict and NOT_RUN here.
