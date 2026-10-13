# WB-29 native context/request design review

12 October 2026. Independent read-only review of `WB-29-context-native-request-design.md` and `WB-29-w2-native-command-design.md` against the current W1/W2, native quote and receipt owners. Root alone authors product. **Design is suitable for implementation subject to the one small bound correction below. This is DESIGN-only; no source implementation, joined acceptance or whole-WB29 completion is approved.**

## C29-D01 — Reason is 2000 UTF-8 bytes, not characters

The context note's “native2000-character limit” is inaccurate. `supabase/migrations/12102026_2044_banking_pay_next_independent_component_resolutions.sql:31` constrains decision Reason with `octet_length(reason)<=2000`; its event at line82 requires 1–2000 bytes. Current `supabase/repeatable/12102026_2218_banking_pay_next_resolution_intent_candidate_guard_v1.sql:73` preserves that byte limit. A multibyte Reason can fit 2000 characters and still fail the native insert/quote boundary. Smallest correction: state and enforce **2000 UTF-8 bytes** before populating the composite; keep manual nonblank qualification and nullable Suggested/CLEAR with a finite native audit reason. No schema change is needed.

## Checked design boundaries

- **Read authority:** a private STABLE, security-invoker context helper under the existing public service/admin/NEXT/actor boundary is appropriate. It must stay write-free, including transitive helpers: no seed, row creation, refresh, clock allocation or lock-taking current2146 bound snapshot. The original public authority/adoption gates remain outside the new helper. Missing control/authority is unavailable, never manufactured.
- **Native policy and identity:** each Work uses its immutable approved revision/basis policy and exact applicable policy window/date. The common target is actual Candidate/Umbrella/VAT; original Source channel comes from the revision. Copying EX08's current policy-control pointer would be wrong and is expressly excluded. Current/applied/Position, physical Timesheet/version, Source-book tuple, metadata/FVR/workbench/adoption/clock operands are genuine native fields. Contract availability and frozen approval/Daily availability remain distinct.
- **Closed intent:** exact line/family/code plus explicit nullable intent and decision pair supports W1 context without an invented choice, W2 SAVE with one native mode, and CLEAR of only an exact disclosed decision. Raw finite range and six-decimal/penny precision must precede numeric/composite casting; NULL/blank is not zero. Duplicate/empty scope and extra keys reject. Arbitrary native Additional codes remain intact.
- **Receipt compatibility:** the existing2217 action variant permits the proposed flat `request_body.works`, original action/actor/Candidate/context/Reason/pairs/intent and `zero_confirmed` operand. Its schema does not replace strict caller validation. One real Work retains its FK; many Work requests use NULL; resolution job and all retry fields stay NULL. Preserve the retry action's original shape and 4/8KiB limits. The complete W1/W2 envelope remains 32KiB; 100 decision rows is a ceiling, not a promise every such request fits. W1 response120000 and receipt result64KiB are separate bounds with no omitted rows.
- **Replay and locks:** receipt replay/readback precedes fresh context/currentness. For new W2, Candidate followed by all explicit Works in UUID order, then Positions/finite related Cases, must be acquired before seed/refresh/publication/global clock. Re-derive and compare disclosed context/pairs under those locks before own effects advance it. Source READY remains untouched; UNBOUND needs the original exact witness and full Position scalar coverage. Treatment-root qualification does not relax actual expected/current/applied or E2 guards.
- **One quote owner:** current2218 is STABLE and accepts an in-memory native intent with server-qualified basis/Position/included Source. Current saved wrapper2213 preserves its live or retained-run identities before delegation. W1 and W2 may reuse this path; no dummy persisted decision, browser financial authority, rate/VAT/ERNI formula or hidden linked expansion is justified.
- **Zero consent:** one resolution-level literal boolean captured in the original W2 request is sufficient only for the exact disclosed scope/values. Qualified selected zero rate/amount/result is distinct from unavailable NULL. W1 Preview requires no consent. WB31 still owns the actual control; no automatic consent or separate protocol is introduced.
- **Scope:** present rate fields named by the note exist in current Contract/native approval sources; contracted/contractless Daily has its genuine captured authority. Future WB30–34 discovery, evidence, screen and adoption integrations remain explicit follow-on obligations, not claimed by this design review.

The newer context note refines the older W2 note's abbreviated row spelling and adds the zero operand; implementation must use one exact closed schema consistently on public/Worker/Office sides, not accept both aliases. No further essential design defect was found in the checked boundary.

## Additional native disclosure check — C29-D02

The current2044 ACTIVE uniqueness is qualified by target channel/Umbrella/policy/date (migration:61–70), so several incompatible ACTIVE versions can map to the same present line/family/code. Current2100 `active_v1` (189–229) intentionally finds only the exact current target/VAT/policy/date and refuses a duplicate compatible match. A generic unqualified one-pair selection must not replace that contract.

Root's proposed refinement is appropriate: disclose the compatible pair for normal SAVE separately from a bounded `incompatible_decisions[]`. Enumerate only exact requested current-boundary original/published mappings through the existing Work/boundary and Work/published indexes; deduplicate `(id,version)` if both arms match, use deterministic order and an overflow sentinel plus encoded response cap. Include actual target channel/Umbrella/VAT, immutable policy ID/date, Reason and literal clearability. Do not choose “latest” across target variants or silently truncate a clearable scope.

CLEAR must echo its own exact disclosed ID/version, whether compatible or incompatible today. Under the original locks it must prove ACTIVE state, Candidate/Work/family/code and exact current line/basis mapping, without imposing today's target/date compatibility on that old decision. Never replace the selected owner with the current compatible owner. Revalidate the disclosed bounded owner set before mutation and preserve the existing compatible-sibling carry/refusal contract. If one CLEAR submits several different owner versions on the same bucket, duplicate validation must distinguish those exact owner IDs; SAVE still rejects competing intent for one financial row. This is an action-specific identity qualification, not another protocol/store.

The native Position operand is `applied_revision_id`, not an invented applied-version column. Missing real worker-control or Candidate-metadata authority must refuse; no `coalesce(...,0)` may manufacture it. Native Source-book revision supplies its own version separately.

**Updated design disposition:** the two small clarifications C29-D01/D02 should be written into the root design before implementation closure. The proposed native disclosure refinement is suitable; exact product source review remains required. No new schema, owner, route, history sweep or financial calculation is needed for these corrections.

## Saved evidence

| File | SHA-256 |
|---|---|
| design/WB-29-context-native-request-design.md | `0efdfca1a98bf0458ba1f8e3a8400714922f7294182f2057ff7df91a938d022b` |
| design/WB-29-w2-native-command-design.md | `dd9e0a5bfcd187c4b487da24cb2354b3911ad4bff2cb8a18e15c41df5a207744` |
| repeatable/12102026_2218_banking_pay_next_resolution_intent_candidate_guard_v1.sql | `1f161c8a632ded5985bac8cd37db7cca9de189c7275b9e2309fe423e98f67a89` |
| repeatable/12102026_2213_banking_pay_next_resolution_input_quote_v1.sql | `37d3df35bc6297c1b6695b765476965b5ad024bd28af469e7beb49bb469a5c2a` |
| migrations/12102026_2217_banking_pay_next_resolution_command_receipts.sql | `d8284aedeafd2259f768a217bd05ff5ac666a9faea1bd4aacce87714e2e5a6cc` |

Paths in the last three rows are under `supabase/`; design paths are under `docs/bpay-next/`. Current saved bytes govern. Review performed file reads/comparisons and this Markdown write only; no product edit, SQL/RPC/database action, fixture, test, render, hosted action or commit.
