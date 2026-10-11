# DR-10 independent design review — BD111

11 October 2026. Read-only review of the full `DR-10-design.md`, SHA-256 `02cce93b5c43d1357257c83af57826844e931a5dd66b34d099d8aa7a8a4dd051`. **PASS for the narrow architecture, with the implementation conditions below.** This is a design verdict, not a built, installed, joined-runtime or visual acceptance claim. No product/checklist edits, SQL/database/hosted calls, tests, fixtures, rendering or commits were performed.

## Authority and original cut

Read full PLAN `03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:274–292`, all local `IMPLEMENTATION_CHECKLIST.md:2912–2937` DR-10 rows, PLAN `review/R3.md:100–107` / DR-10 R3.json, full EX-01 card `04_EXECUTION_AND_OUTCOMES.md:102–128`, the saved current investigation and final-definer registry. Current task owner E1 / CHECK-A1 and W13 directions govern the already accepted descriptor flow.

The combined PAYE_GUARD_DECISION request is the necessary small adjustment to the old card's decision shape: it performs the **original** descriptor Create with the original run/PREPARE identities, rather than changing frozen membership after Create. Current Create0426:506–587 writes per-Work E1 skips before selection/choice insertion and freezes the accepted descriptor at626; accepted_choice_guard1832:27–31 then forbids skip changes. The proposed six-argument private overload, original five-argument delegation and untouched three-argument initialization preserve the existing cut and shared public boundaries. No second Create, later live channel refresh, capture-time hold gate or economic rewrite is justified.

UMBRELLA_ONLY must write the exact PAYE_EXCLUDED_BY_SAME_WEEK_GUARD skip before accepted Work insertion, remove associated out-of-channel Case eligibility before Candidate capture, and leave no new holds or REVIEW outcomes. CHECK1048 needs the same exact additional code in both actual-old-literal retention branches, preserving the rest without rewriting stored rows. Native Candidate settings and original query snapshots remain captured once.

## Essential implementation conditions

1. **Close VERIFY_PAYE membership receipts explicitly.** The prose says to admit PAYE only without mislabelling Umbrella Work. Omit those out-of-channel members from the accepted member census *before* its numbering/counters/freeze, or use an explicitly truthful native exclusion representation. Do not retain a null-skip Umbrella WORK while omitting its selection/choice: current ack_choice_work_page1832:603–617 then requires that exact original membership and raises CHOICE_ACCEPTED_MEMBERSHIP_MISMATCH. Apply the same channel cut to Case membership and all maintained counts. This needs no invented Umbrella-as-PAYE skip code or later rewrite.

2. **Make the finite guard/proof bounds concrete.** Same-week facts, reason and complete policy intent must fit the existing 2048-byte auth intent and bounded response. A list of every same-week root/legacy obligation is not inherently bounded. Use a deterministic native finite budget and an honest overflow/pending refusal; do not truncate away blockers or sign only an arbitrary displayed prefix. Keep the existing server-computed scope owner and its digest; no new witness, client hash, preflight store or protocol framework. The four channel labels/counts must come from the same exact original descriptor/count context, never all-matching counts substituted for a ticked subset.

3. **Preserve fresh admission and replay ordering.** Source/module/sorted Candidate and Work parent locks remain before original command_clock UPDATE (Create0426:268–331); first admission must execute its native guard read after that lock. A UI snapshot or a pre-lock cached result cannot be reused as the serialized check. Count/proof and current HEADER qualifiers must match the original descriptor. Accepted replay compares the stored descriptor/actor/command/policy and retained scope, then uses original consume semantics; it must not rederive a new guard scope, change cutoff or demand a new proof after the original proof expires.

These are direct implementation obligations, not requests to broaden scope. The architecture can proceed within them.

## Locks, guard identity and retained facts

The existing command clock serializes first accepted Create, and captured PAYE membership in live PREPARING/REVIEW roots closes the concurrent Create gap. The guard must exclude its own run, cancelled roots and genuine non-PAYE membership, using accepted official pay_date Monday through Sunday, not Work week. Original pay-date range index0370:70 already exists; no additional index is proved necessary by this review.

Seal2306:635 locks its run; enqueue1832:1206 then uses the existing run/command path and explicitly avoids command UPDATE during receipt replay. Keep the seal guard as a fresh **plain read** under that original root lock. Do not acquire the command clock, other same-week root locks, sorted business-parent locks or a fresh proof-row lock from this later seam. First acceptance already serializes and stores the policy. A fresh hard blocker still refuses Verify; exact sealed replay returns its original receipt before new policy checks.

Confirmed root status alone does not prove execution. Qualify actual PAYE worker/transfer/native request facts; mixed, unresolved or genuinely never-executed active membership remains a hard block. REFUSED or internal-final display labels must qualify through their genuine native financial identity/history, rather than being treated as execution evidence solely by a label.

The conditional legacy branch is appropriately limited to actual retained PAYE obligations and their coalesce(authoritative_payment_date,pay_date) week. It must not blindly copy the legacy helper's global Draft search or adopt unused shells. F-18 readiness records hostedCounts NOT_OBSERVED, external quietness UNPROVEN and activation false; owner clean-start authority is not observed absence. This design correctly retains final-release factual census/disposition as a prerequisite without manufactured fixtures, old-engine invocation or an asserted zero.

## Transport, reauthentication, count disclosure and UI

The policy proof binds actor/owner epoch, original run/PREPARE UUIDs, native namespace/official week, workbench/adoption revisions, decision/channel/reason and current guard facts. It authorizes the named same-week exception, not a full tick set or guessed financial amount. This interpretation is explicit in BD111 and fits EX-01's server-computed finite scope without a persisted preflight table. The full bounded descriptor and existing financial owners still determine all captured economics. Native WB12 HEADER count_selection plus selection_memory (`FE js/bpay-next-workbench.js:1121`; native reader0113:427–435) supplies actual selected Candidates separately.

The EX-01 policy-preview variant must be closed and truthful: action/consequence/count/context, with no fabricated zero total or Candidate identity. First dispatch must verify actual token signature/actor/action/scope and configured factor policy, record only the native proof UUID for SQL, and consume atomically under the business-lock order. Current scope1302:26–40 and Worker qualifier/command maps do not yet implement this action; its presence in the protected list alone is not delivery. Consume1302:343–359 already supports identical consumed-command replay after expiry.

The proposed larger GET envelope is **GUARD-only**; current Worker pageCall283 uses the common32768-byte URL-search limit and FE Office548–552 stringifies scalar page fields. Product must explicitly encode/decode the descriptor as JSON, enforce both encoded and decoded bounds in FE/Worker/SQL, keep other page/mutation limits unchanged and fail known oversize before dispatch. Source design does not establish end-to-end acceptance of a128KiB URL; that remains an honest transport/runtime acceptance limit, not a reason to widen unrelated routes.

The existing WB21 journal retains original IDs, descriptor, policy and exact pending bytes before dispatch. Cancel performs no financial action. Known stale refusal preserves ticks and requires explicit Refresh/recount; unknown outcomes retry identical combined bytes, never a new cut/UUID/proof. The supplied SCR39/39b choice order and card-required reason/confirmation are preserved; hard-block state exposes only Umbrella only and Cancel. No early official date/gross, account-verification Create gate or guessed paid reference is added. Full42 actual A4 review and joined auth/concurrency/queue/payment checks remain pending / AUTHORED NOT_RUN.

## Review boundary

Current parent sources are Create0426, accepted choice/ACK/enqueue1832, seal2306, manifest0454, public intake/page0814, proof1302 and current closed Worker/Office/WB12 modules, as listed in the saved investigation. The design adds only the necessary guard/action/closed policy proof/channel audit support and unchanged money-owner call plumbing. No essential architecture replacement, new table, week lock, financial command kind, producer rewrite or public financial exposure is required.

