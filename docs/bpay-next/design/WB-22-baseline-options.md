# WB-22 precise NONE baseline options

Read-only design investigation, 10 October2026. No product edits or execution. Recommendation: retain an atomic **server row baseline of all currently eligible native identities**, including implicit NONE unticks, at the original accepted Create. Store those rows and their scalar header inside the two named WB-22 memory tables; acknowledge the already captured generation at seal. No new clock, economic command, money/producer owner, financial snapshot or proof framework.

## Existing acceptance already holds the required parents

Current original `supabase/repeatable/10102026_1832_banking_pay_next_accepted_choice_intake_v1.sql` is decisive:

-218–225 preholds every Candidate having scoped selectable Ready item, plus explicit override Candidates, FOR SHARE.
-236–242 preholds every Work having scoped selectable Ready item, plus explicit Work overrides.
-243–249 preholds every relevant immutable Work revision, including overrides.
-250–256 preholds every relevant Case component, including explicit Case overrides.
-257–268 preserves earlier-economic-command refusal and original command-clock acquisition/reprobe.
-294–307 checks authentic agency/actor/epoch/date plus exact Workbench/adoption context.
-338–347 rejects any late unheld parent.
-349–405 builds financially selected Work/Case members in one statement; **NONE/explicit-selection and exclusions are applied only here**.
-490–494 rejects a changed current projection context before commit.

Consequently the omission of unticked Works from run_choice_member is not an absence of original scope/locks. A small same-acceptance INSERT can capture its current eligible identities **without** the financial selection predicates367–369/386–404 and without modifying run_choice_member, run_selection or enroller semantics. It must retain the same native scope/current/complete/coherence conditions358–364/378–383. This is memory evidence only: it does not pay, reserve, enroll or create hidden positive Work choices.

## Concrete two-table shape

`private.bpay_next_selection_mode_memory` may contain a finite discriminated header/member layout, avoiding an extra table. For example:

| Row kind | Minimal retained fields | Natural lookup |
|---|---|---|
| HEADER | Full agency/epoch/actor/date/normalized scope; original accepted_run_id; actual command_id/agency_sequence when present; ALL_MATCHING/NONE; immutable descriptor/local intent revision; capture time; baseline member counts; nullable acknowledged-at marker | Namespace + accepted_run_id + HEADER; latest genuinely acknowledged header ordered by original accepted agency_sequence, not wall-clock seal completion |
| WORK | Same accepted_run_id + Candidate/work_id + expected_revision_id + native source/root identity if needed; finite exact fixed typed basis JSON/version | (accepted_run_id,WORK,work_id) |
| COMPONENT | Same accepted_run_id + Candidate/work_id + component_key + exact approved_line_id/expected_revision_id; finite exact fixed typed component basis JSON/version | (accepted_run_id,COMPONENT,work_id,component_key) |
| CASE_COMPONENT | Same accepted_run_id + Candidate/case_id/case_component_id/component_revision/rule_id and precise native fixed typed basis JSON/version | (accepted_run_id,CASE_COMPONENT,case_component_id) |

Native Candidate default is derived from that Candidate's typed eligible leaves, not a single opaque Candidate flag. Add a Candidate row only if needed to retain exact intentional Candidate exclusion; it is an intent parent and must retain/resolve its accepted leaf scope. A Candidate with no eligible leaves at the prior boundary has no old eligible leaves to suppress; genuinely later Ready leaves are new.

`private.bpay_next_selection_exclusion_memory` stores only deliberate Candidate/Work/component/Case override intent with the same full namespace, accepted_run_id, actual typed stable identity, selected/excluded meaning and its exact baseline reference. Natural uniqueness must distinguish the four kinds, stable identity and generation. It can reference the mode-memory header/member rows with typed constraints. Baseline rows need only identities/version/fixed typed equality terms, **not financial amount snapshots**. Current native owner amounts are used only as finite comparison inputs if genuinely relevant to basis, never as accepted payment authority.

HEADER plus member insertion is one atomic acceptance transaction. A capture-complete header/count means the original full scoped set was retained; it is not an asynchronous latch/enrollment protocol. No100-row partial-baseline-as-success:1200 existing eligible Works need1200 native baseline rows even if NONE ticks only2. This does not put1200 IDs in the descriptor/browser. The existing Create already scans/preholds that full admitted scope; capture adds a set-based server INSERT of those actual rows. Native request/statement budget failure rolls the entire acceptance back, not partial memory. Later reads remain≤100 indexed native seeds with only exact baseline PK joins.

## Exact basis: exclude presentation churn

A baseline key answers *same native member?*; direct fixed typed JSON/version equality answers *unchanged selection facts?*.

- Work: exact immutable current/applied approved revision identity and existing `private.bpay_next_revision_worker_basis.whole_basis/expense_basis` (1100 schema36–40), plus the finite genuinely applicable current controls/position facts. Revision change is a real changed Work even when amounts match.
- Component: stable Work/component_key identity; exact expected revision/approved_line_id plus existing immutable component/segment approved basis. Preserve arbitrary Additional/mileage identities. Compare relevant native position/reservation/residual/control values through direct fixed typed JSON/version equality, without grouping different Work/component origins.
- Case: actual case_component_id + component_revision + rule_id/rule revision and native current instruction/direction/payroll stage/channel/resolution/appropriate outstanding/reservation basis. A Work revision cannot represent it. The direct fixed typed equality basis must include the real component counters relevant to selection; a rule revision alone does not change when an outcome changes outstanding debt/credit.
- Candidate-level exclusion: applies to unchanged baseline leaves. A newly introduced/changed Work or Case leaf may default selected even while old leaves remain excluded; do not tick the whole Candidate and silently include every unchanged excluded child. Candidate/group checked/partial status is derived from its current eligible leaf defaults; actual three-state control ownership remains WB-24.

Do not hash/compare names, TMS reference, client display, bank-warning copy, updated/audit timestamps, projection/adoption revision, is_selected/accepted-run display fields or transport progress as financial change. Current1429:479–500 advances group/item/summary projection_revision solely for Candidate name/reference/bank-warning metadata. Per-Candidate worker_control.financial_view_revision alone is also too broad: unrelated Case commands can advance it while excluded T remains unchanged. These may be coherence guards, not the equality basis.

The fixed typed JSON/version tuple is an equality aid for actor intent only. Define its finite exact fields once and compare directly with IS DISTINCT FROM, used identically at capture and current read. No new digest/hash, certificate, financial snapshot or producer version authority. Use native identities/versions wherever sufficient; include only actual comparison terms required by selection intent. Approved revision/rates are already immutable authority; no later producer rewrite is required.

## Default and changed predicates

Let H be the latest acknowledged exact-namespace header and B be its retained row for current native leaf L:

- H absent: all current eligible Ready leaves selected, no fabricated last-Draft badge.
- H.ALL_MATCHING: selected unless an explicit exclusion matches the precise unchanged accepted leaf basis. A absent/changed B is new/changed and selected; stale explicit exclusion is visibly reconciled.
- H.NONE: unchanged B defaults unselected; absent B is new; different actual basis is changed; both default selected. Deliberate later local override at the current precise basis wins.
- Candidate exclusion: suppress only matching unchanged baseline leaves, preserving new/changed siblings.
- Component/Case exclusions: use their own accepted identity/basis, not their Work/Candidate root alone.
- Current native unknown/dirty/unsupported evidence remains the existing typed unavailable/refresh state, never selected financial success or money zero.

Indexed anti-join makes unloaded new rows truthful: current leaf with no `(H.run_id,kind,stable_identity)` baseline is new. Equality comparison detects changed retained leaves without history scans, guessed timestamps or a global projection boolean. Removed baseline leaves simply stop contributing; if the same stable leaf returns with same exact basis it remains unchanged, while a new immutable revision is changed.

## Minimal scalar descriptor/reader seam

A reopened NONE store cannot enumerate every implicit old untick into1000 browser overrides. Return one exact `memory_baseline_run_id`/mode scalar in existing HEADER context plus actual per-row default/new-changed facts in the named bounded reader branches. Accept that exact scalar in the existing Create descriptor as a small metadata reference; validate acknowledged state and full namespace. Existing native CTE selection can join the immutable baseline to apply effective default leaves before current finite explicit overrides. Once the new Create is accepted, its resulting native positive membership is fixed exactly as today; later upload/seal never recomputes defaults.

This seam is necessary if NONE reopens with new/changed unloaded members: unchanged raw descriptor modeNONE currently selects only explicit Candidate/Work/case true entries at1832:367–369/402–404. A badge/client checkbox alone would not include an unloaded new Work, while automatically setting Candidate=true would wrongly include unchanged excluded siblings. Server effective-default application is the bounded truthful alternative; no new financial kind/read kind/route or full browser ID vector. Existing HEADER count_selection needs the same baseline reference and finite explicit override semantics so displayed server counts agree with accepted selection. It remains count/default ownership, not browser monetary calculation.

A fresh user Clear-all operation drops the inherited baseline reference and means NONE at the current acceptance, rather than immediately reapplying an older NONE baseline's new/changed defaults. The subsequent accepted Create captures its current whole eligible baseline; genuine seal acknowledges it. Existing local `setMode` clears all four arrays. Keep uses bounded local unsubmitted intent; it does not create/acknowledge server baseline per click. Precise persisted reset/delete remains later WB-28 ownership.

## Acknowledge / replay / race

- Original accepted replay returns the original memory generation; no recapture from current state.
- Before seal, new header/member rows are pending intent evidence, not the active remembered baseline.
- Existing seal transaction acknowledges the exact accepted header/generation atomically after native upload/receipt/count validation. Sealed replay is no-op for current baseline selection.
- Concurrent Drafts for the same namespace select latest genuinely acknowledged *accepted agency_sequence*; an earlier accepted run sealing later must not replace a newer baseline. No new global memory lock is required merely to compare this immutable order.
- Empty/no-selected-Candidate Create still receives no PREPARE command under1832:495–505. Preserve that rule; its pending memory capture cannot be treated as a completed Draft/acknowledged baseline. Local NONE Keep still works.
- Current mutable native comparison reads must share the original Create parent-lock/snapshot/context closure. Insert all baseline rows in the same statement snapshot as, or immediately adjacent to, selected membership under those locks; final authentic context recheck remains. No new meta/FK/business lock after command clock.
- No late paged capture of today's rows after acceptance. Such pages would need a forbidden historical version reconstruction/protocol; simply retaining rows at the already-existing acceptance avoids it.

## Options rejected

| Option | Concrete failure |
|---|---|
| Only accepted NONE run_choice_member | Omits implicit unticked Works/components/Case rows by original selection predicates; old omitted rows become falsely new. |
| Single Workbench/projection revision watermark | Metadata-only name/ref/bank-warning advancement falsely marks financial rows changed. |
| Work expected_revision_id only | Misses Case/component/control/outstanding changes; not their identity. |
| Candidate global financial revision | Any unrelated Candidate Case command can retick unchanged Work exclusions. |
| Timestamp/approved-at cut | Current Case counters and controls are mutable; audit times are not economic version proof. |
| Browser IDs for every NONE untick | Violates bounded intent/unloaded semantics; exceeds1000/32KiB caps and fails new members. |
| New maintained producer version framework | Unnecessary: same-acceptance baseline rows and fixed current native PK comparison suffice. |

## Source references

- `supabase/repeatable/10102026_1832_banking_pay_next_accepted_choice_intake_v1.sql` SHA-256 `b629fcc003b76ae61ec58c0d35ad2256a4cf9f736e307f5dc80e0a8fb0e20438`.
- `supabase/repeatable/10102026_2232_banking_pay_next_selection_pages_v1.sql` SHA-256 `cc1491f55ff1f4afe48f82e8892015aa2f91500f42d0d41d692f0e6b78dd3e48`.
- `supabase/repeatable/10102026_2240_banking_pay_next_selection_receipt_cleanup_guard_v1.sql` SHA-256 `36c7a3aac98fc13431e03f4534ba3ca1242cef2fc735775ebe890e412bb1fb45`.
- `supabase/repeatable/10102026_2245_banking_pay_next_subset_upload_context_v1.sql` SHA-256 `e1c9936055a24b271b887ee431a8f8dd4e2a224d1b8bb184f3e709534e1fabb4`.
- `supabase/repeatable/10102026_1429_banking_pay_next_workbench_current_continuation_v1.sql` SHA-256 `dc539089fae0cd1a8baf4765a89cea9ff1d1e8039200ecda5d80371dda2335dd`.
- `supabase/migrations/10102026_1252_banking_pay_next_workbench_summary.sql` SHA-256 `aa9a42864893856c9ae02479edb1172cdf39c764a6ca2e9e77f2fd11253affab`.
- `supabase/migrations/10102026_1100_banking_pay_next_segment_exclusion_v1.sql` SHA-256 `0a2e8a72f43bd6de767e0b277c8c895e02ed29482588a40c23cd51ebd8ac5ef9`.
- `supabase/migrations/04102026_0300_banking_pay_next_case_instruction_core.sql` SHA-256 `04b44425d288e714037012c3b4eb4d27679137905e1c380682812cc8273ad40e`.

## BD077 direction — supersedes the optional clock discussion

Root selected the atomic row-baseline option in the two named tables. HEADER+LEAF layout retains direct finite typed native basis JSON/version terms at original Create and acknowledges only genuine seal. No new hash/digest or maintained projection basis clock. No existing projection/money/producer owner changes. The rejected maintained-clock discussion is stopped; row baseline itself supplies both old membership and precise equality without an agency-wide boolean. This note now uses direct JSON/version equality language.

Remaining implementation design check: effective defaults and header counts must be leaf-specific. A changed Work/Case leaf must not implicitly tick every unchanged sibling under a Candidate. If only one existing component basis changes, an effective whole-Work ALL choice cannot silently select unchanged excluded components; server derived positive membership/subset upload must follow its actual native owner, while WB-25/WB-26 control UI remains later.

## Independent review of the narrowed HEADER+LEAF proposal

The parent's stated BD077 proposal is consistent with card D02/D03/D04 and Rules865–878: atomic current eligible baseline at accepted Create; exact typed deliberate exclusions; actor namespace; no per-click writes; mode scalar references original run; seal acknowledges only retained accepted intent; bounded current reads compare exact leaves. No replacement of original revision/pin/clock or producer maintenance is needed. The saved full root design was not yet available at this snapshot; this verdict covers that stated proposal, not an unseen file or implementation.

Specific design requirements to keep concrete:

1. LEAF captures all original scoped eligible native members independently of NONE/exclusions, including Work/component/Case kinds. Current originals already prehold their parents218–256. No financial run_choice_member selected flag is added for these memory leaves. HEADER identifies atomic capture completeness/counts; later paged readback cannot manufacture the initial cut.
2. Stable identity is native Work plus component/segment key, or actual Case component; not transient Workbench item UUID or presentation portion. Fixed equality tuple may retain exact expected revision/approved IDs, rates/basis, applicable channel/Umbrella/control facts and real native counter/version terms. No display/audit/projection/adoption/accepted-reservation presentation fields. Missing/unknown native basis cannot be normalized to zero/false or declared unchanged.
3. Current comparison occurs only after existing W1 coherence/current-read admission. An old LEAF missing for current eligible identity is new; an unequal finite typed tuple is changed. Candidate exclusions map to unchanged accepted children, not all future children. Native group current data and bounded seed supply the actual identities; no mutable latest-history lookup.
4. HEADER counts, displayed per-row defaults, freeze descriptor and accepted Create's actual positive Work/Case membership must agree. One scalar acknowledged baseline reference plus bounded explicit overrides replaces browser enumeration. A fresh local Clear-all drops inherited reference and remains truly empty; unchanged siblings must stay unticked when only one leaf is changed. Required implicit component/subset membership continuation must be designed explicitly, not claimed from a New/Changed badge.
5. Empty/no-selected Create still has no financial command. HEADER may reference its actual run, but must not FK a nonexistent planned PREPARE UUID or treat the EMPTY outcome as completed Draft memory. Only genuine seal sets acknowledged state. Exact replay never recaptures; later older seal cannot win over newer accepted sequence; pending/unknown preserves local/journal state.
6. Header and typed rows are namespace-private with FORCE RLS/owner ACL, unique native keys and FK/CHECK shape. Reads use existing kinds/routes and≤100 seed rows; no full agency arrays, new digests/triggers/producer owners or second protocol. The only new API/descriptor field should be a finite typed acknowledged-baseline reference if required for exact server default/count parity.

Card coverage is complete for this design direction provided these finite semantics are included in BD077. Later WB-25/WB-26/WB-24 UI controls and WB-28 reset remain later owners; exact typed retention/default truth is mandatory now, not a placeholder for those future controls.
