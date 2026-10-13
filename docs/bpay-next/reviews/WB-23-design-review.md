# WB-23 design review

Verdict: the finite preparation-intake/retained-membership design is sound in scope. One concrete premise must be corrected before treating the proposed acknowledgment metadata as indispensable: installed1832 already orders all SUBSET members first. This is a design source review, not product implementation/installation/app PASS. No product edits, SQL/database/runtime/tests/fixtures/browser or hosted calls.

Reviewed root `docs/bpay-next/design/WB-23-selection-pages.md` raw SHA `1924bddf7c06160bfa0e3f69ae394a9d651593f06b0f096f0b386625b3698ef9`, full WB23 card/all rows/R2 and exact native1832/2026.

## F1: use the actual existing subset prefix

Create1832:349–370 already numbers WORK members with `(selection_mode='SUBSET') DESC, work_id` at353. Required SUBSETS-before-MATCHING does not by itself need out-of-order member acknowledgment. ACK602's member_no=uploaded_choice_work_count+index and614's native selected ordinal remain correct if SUBSETS use this original prefix, including skipped attempted members. Select-all then walks the remaining ALL suffix. This preserves actual first_selection_no/count semantics, native seal completeness and accepted-choice immutability without adding a new mutable field/guard/FK.

Smallest recommendation: expose the existing bounded retained-input WORK_CHOICES variant to drive SUBSETS in immutable member_no order, continue from original uploaded_choice_work_count for matching, and keep one current page. Reject MATCHING until every required eligible subset's components/segments are genuinely sealed. A captured subset skip has no work_choice to seal and is acknowledged with its real skip receipt. It must not be converted to ALL or omitted from attempted counts. If root identifies a concrete required out-of-order caller, the proposed acknowledgment_page_no/FK remains a finite alternative, but it must additionally preserve first_selection_no meaning and exact totals; do not call that new field necessary solely from the seven-phase order.

The earlier investigation's interspersed-SUBSET assertion is explicitly corrected in its appended section. It arose from incomplete source reading, not an installed owner defect.

## Other design contracts confirmed

- Existing accepted capture/exclusions/work_choice reservations stay authoritative. Matching is a preparation action, not a new financial command. Literal skip-if-work_choice-exists is invalid because1832 precreates all choices; skip only already uploaded subset prefix while never creating duplicate ALL. No parked resolver, extra query snapshot, financial clock, settings capture or live producer changes.
- EXCLUSION_PAGE validates the exact preaccepted Candidate-first/Work-second page assignment, stores immutable request/outcome even when empty, replays before fresh closure checks and rejects changed page body. First actual matching receipt/seal closes exclusions; ordinary subset pages cannot close them. `max(1,ceil(n/100))` empty-set completeness is a justified descriptor-only seal refinement, preserving the legacy branch.
- Empty matching pages need real atomic input/cursor/result receipt and0–100 constraints/guards only for descriptor matching. Existing legacy WORK_PAGE remains1–100. No immutable page UPDATE to attach an outcome; fresh input/output inserted together. Only-skipped pages retain actual attempted IDs and captured skip reasons.
- Exact matching cursor must equal the retained original progression, not merely be a valid bigint. Bind the first cursor to the acknowledged subset prefix and each later cursor to the prior matching receipt. Restore the original complete/count/skips on replay. An arbitrary future ordinal cannot skip members and claim complete. Native unique(run_id,member_kind,member_no) already supplies indexed ordering; use <=100 or limit+1 before constructing row JSON.
- Original actor/admin/NEXT/module/run/accepted-descriptor authority applies on every page; no new permission per checkbox. Before Create, current/coherent Workbench controls govern ticks; after Create, membership comes only from immutable run_choice_member. Post-click pointer changes do not alter matching membership, and existing RuleB PIN REVIEW behavior remains unchanged.

## Necessary WORK_CHOICES retained-input variant

This is genuine minimal support for NONE Candidate-wide ticks and captured skipped inputs. Existing normal2026:761–766 reads only work_choice; skipped attempts have no such row and current UUID order is not accepted member order. It cannot select Candidate-implicit membership truthfully. Same public signature `public.bpay_next_preparation_page_v1(uuid,text,jsonb)`, same WORK_CHOICES kind and same route can host a discriminated branch. No ACCEPTED_* kind, new public route or service signature is needed.

Recommended finite interface, to be finalized by root before implementation:

- Alternate closed args: run_id, `accepted_inputs:true`, nullable `after_member_no`, limit1–100. Keep original normal args run_id/after_work_id/limit exactly unchanged; reject mixed cursors or false/unknown variant flags rather than silently defaulting.
- Alternate row fields: member_no positive decimal text, candidate_id UUID, work_id UUID, expected_revision_id UUID, selection_mode ALL/SUBSET, skip_code nullable exact existing captured code. Every value comes from the actual retained WORK member. If acknowledgment metadata is adopted, expose only its actual nullable page reference; otherwise use the existing aggregate native prefix as a separate actual header/cursor fact when needed.
- Do not manufacture selection_state/page_count/component_count for skipped rows to force them into the old six-field WORK_CHOICES schema. The old shape is for actual work_choice. BE/FE schemas choose the exact alternate shape from original args; unknown fields/types refuse. Work/body correlation, native order and exact last-member continuation are validated on both sides.
- Gate the variant by actual accepted descriptor, original run actor and module epoch, with explicit missing/stale refusal. Never derive it from CURRENT_WORK/CURRENT_CASES or current Workbench facts. Returning captured IDs is a read; it performs no materialization or acknowledgment.
- Use the same bounded rows/complete envelope and finite typed member cursor produced by the Worker from the actual last row; empty terminal page is legitimate. Page-time Work/revision updates cannot change this immutable order. No all-ID array, member scan at each tick, amount or current eligibility read.

Latest2026 full public preparation reader raw SHA `278bd4d3dccc1e7453a69d1d2e0e4bdcde7075e5fe5181c5fe090c4a9dfc7e8a`. Required three-way successor must preserve COMPONENTS original correction reference and associated rate schedule/available rate fields725–759, SHIFTS stable segment/role/band fields791–804, BREAKS/RATES native details805–819, original Case-component delegation684–686 and all other branches/ACL/120000-byte behavior. Only the WORK_CHOICES argument/selected subquery/transport variant should change. Existing normal post-seal readback stays exact and does not interpret the alternate input tuples as sealed choices.

## Completeness and scope boundaries

The planned seven phases are real work, not phase names alone: ordered subset pages+component seals, exclusions, matching or NONE explicit ALL, then actual later Case/guard/seal/readback owners. Current namespace journal can be extended with compact phase cursor/count/current page and existing exact pending/last receipt. No client all-matching enumeration or unbounded historical receipt archive. A lost mutation reply stops advancement and preserves identical bytes. Global controls call existing local setMode and clear overrides; current accepted journal remains separate.

WB23 must finish its matching/exclusion/explicit dispatch slice. WB22 later owns default/changed-row persistence, WB24 tri-state boxes, WB25 subset user interaction, WB26 retained Case enumeration, DR10 guard and DR02/DR06 progress destination. These later joins remain explicit; their absence cannot become fake complete success. Design line2 “Exactly100” should mean **at most100**, with required empty pages allowed; its “new command kind” wording should say **two new intake actions; no new financial command kind**. These are finite documentation clarifications, not another framework.

No approval question or new testing programme is required. Product implementation checks remain root's separate work; this reviewer has executed none.
