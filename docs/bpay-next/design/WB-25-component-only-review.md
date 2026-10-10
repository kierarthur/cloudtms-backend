# WB25 component-only memory intent review

2026-10-11. **Design recommendation: use component-only raw memory intent, with exact native revision and eligibility validation.** This is a narrow supporting correction to the existing named Create and W1 count owners. No new Work mode, marker, selection engine, memory table, financial authority or producer is required. Implementation has not been reviewed or executed by this reviewer.

Read-only source inspection; only this findings document is written. No SQL, product runtime, tests, app/data, fixture or hosted operation was executed.

## Current source snapshot

| Source | Raw SHA-256 |
| --- | --- |
| `supabase/repeatable/10102026_2306_banking_pay_next_actor_selection_memory_v1.sql` | `3f77bbb57500f1320d3873935c8504264a46e50287be9740267f838f8d1dc750` |
| `supabase/repeatable/10102026_2340_banking_pay_next_selection_count_acceptance_v1.sql` | `545ab73faec8f1331f33986b91ee743340f96be7bcecfb36b8a9c4ed12b38b14` |
| `supabase/migrations/10102026_2306_banking_pay_next_actor_selection_memory.sql` | `51cdfb58b668afb18bfa83083d2a14ac3c33b701a6d635feb2b4e592db8c49bc` |

The approved WB25 card is `02_WORKBENCH.md:952–979`; its sole implementation checklist row is D12, the Work/day/expense/Additional controls in `js/bpay-next-workbench.js`. The listed Create, selection append/seal, preparation transport and journal objects are existing context/reuse owners. Exact small supporting changes needed for honest component ticks are within that named acceptance/read chain; the RuleB pin and accepted financial facts remain authoritative.

## Why the representation works

`private.bpay_next_selection_memory_choices_v1(jsonb,uuid,jsonb)` in 2306:118–130 already resolves a raw component override by the actual approved-line UUID. Without a Work override, that override wins for its own component; every untouched sibling falls through to its existing Candidate or memory/default choice. Candidate false and explicit Work false continue to dominate. A supplied Work SUBSET true retains its existing positive-list semantics. A supplied Work ALL true retains all-but-explicit-untick semantics.

Therefore a day tick under mixed NONE/memory need not invent an ALL true Work parent, which would select untouched siblings. Store the unchanged raw component tuple `{work_id,candidate_id,expected_revision_id,approved_line_id,selected}`. An explicit whole-Work action may still write the existing Work ALL/false tuple. No helper choice-resolution change is needed.

## Required acceptance correction

The present Create parent guard at 2306:373–378 requires every component to have a matching raw Work tuple. It should retain this exact old rule for non-memory descriptors. For a memory descriptor only, distinguish **no raw Work with this Work ID** from **a raw Work exists but its Candidate/revision is incompatible**. Testing only absence of an exact matching tuple would accidentally admit the second case.

For each raw component:

1. Retain the existing immutable native identity checks (2306:362–372): Work belongs to Candidate; expected revision belongs to Work; approved line belongs to that exact revision.
2. If any raw Work with the same Work ID exists, require its Candidate and expected revision to match the component exactly. Retain all prior ALL/false hidden-intent behavior for this matching-parent branch. The existing non-memory branch still additionally requires selected SUBSET true.
3. If no such raw Work exists, require memory mode and one actual current eligible scoped component: Work/Candidate/revision/approved-line must all match the submitted tuple; Work current_revision_id and applied_revision_id must both equal submitted expected_revision_id; the native eligible leaf must exist in the original choice namespace.
4. A changed/missing/non-eligible component-only tuple must refuse before accepting the cut. It must never be silently ignored by the choice helper or rebound to a replacement line/revision. Keep the existing typed refusal vocabulary; no new error protocol is needed.

The eligible-leaf proof can reuse the exact maintained relation predicate already at 2306:29–42: item READY, selectable, amount_known, not is_selected, AVAILABLE/ZERO, complete non-dirty CURRENT parent and item, matching projection and financial-view revisions, and original Candidate/Client/channel scope. For this Work-component path require direction PAYMENT and exact native approved-line/revision linkage. This is a native selection eligibility check, not a new query or financial gate. Apply it to the new absent-Work branch; do not retrospectively narrow matching-parent hidden intent that WB22 already supports.

Use one validation statement over the bounded raw component tuples, with native indexed joins or a materialized existing eligible-leaf relation. No browser-wide sibling list is required. A false component override still requires its actual eligible identity; its false value is an intent, not a manufactured membership.

## Expected revision: keep manifest grouping unchanged

Current memory manifest construction at 2306:406–416 chooses `coalesce(raw_Work.expected_revision_id, l.revision_id)` and groups by that revision. Without a raw Work, `l.revision_id` comes from current maintained native leaves. The strict absent-Work validation above makes it equal the component's submitted expected_revision_id. That closes the stale-intent gap without changing manifest grouping.

**Do not merely coalesce to today's revision and call the component captured.** Currently an old approved line can still pass the immutable identity-only check at 362–372, then fail to match the helper's current line at 122; the helper can ignore the deliberate override and derive today's defaults. The new branch must reject that situation. Merely reading an arbitrary component revision into the Work grouping would also risk conflicting groups for a single Work; exact current/applied equality is the smaller correction.

All deliberate component-only tuples for a Work must consequently bind the same actual current/applied revision. The unchanged original per-Work validation at 493–546 continues to own approval, pay-hold, future-week, Snooze, Source query gate and signed-line eligibility. Do not replace these owners with the new parent check.

## Lock and FK evidence

Create already preholds Candidate/Work/revision parents for every scoped READY selectable row before its native command-clock UPDATE (2306:272–303,316). A valid component-only eligible row is in that preheld set. The final held-parent membership assertion at 393–401 and final workbench/adoption context assertion at 593–597 remain required. No new Work/revision lock may be acquired after the clock to accommodate an otherwise invalid component-only row; refuse if the exact tuple is no longer part of the preheld/current set.

The two memory tables' Candidate/Work/revision/line identity columns are not foreign keys. Their actual FKs are run and actor (migration2306:5,9,34,36); there is no hidden late Work FK lock caused by inserting raw memory intent. Native run-selection/work-choice/component owners do retain their original genuine FK/guard authority. Do not change their guards or ordering.

Existing memory Create automatically derives genuine ALL or SUBSET per Work using selected native leaves, materializes positive component IDs through the original append owner in pages of at most 100 and calls the original component seal (2306:565–581). It later invokes the existing segment materializer (609–610). Those owners remain unchanged. No browser sibling enumeration or acknowledgement marker is needed.

## W1 count parity

Current public W1 (`public.bpay_next_workbench_page_v1(jsonb)`) in 2340:145–148 has the same mandatory raw Work-parent requirement. Its existing native validation at 150–155 already checks exact Work/Candidate/current/applied revision and approved-line membership. Relax only the parent-presence condition using the same three-way distinction: matching supplied parent, conflicting supplied parent refused, or genuinely absent parent admitted with exact eligible native identity.

Use the original memory/choice namespace when validating a carried intent; do not reject a hidden raw override merely because a Candidate detail reader has narrowed its display scope. Create uses descriptor namespace.scope; W1 retains the explicit memory reference's original scope where supplied. Count still counts only its own requested scoped native choices. No new read kind or route is required.

Keep real Candidate-exists checks, existing 32 KiB/identity-count bounds, exact baseline resolution, stale context handling and current helper-selected native counts. Known malformed/conflicting identity remains a request refusal; changed actual revision/eligibility remains an honest stale/unavailable refusal rather than zero or positive success. Changes to these installed owners must be new successors and registry entries, not edits to 2306/2340.

## Finite implementation/review boundary

Required: narrow Create memory-parent validation successor, matching public W1 count successor, and D12 UI wiring to the existing choice store using exact observed native component basis. Reuse the current helper resolution and ALL/SUBSET append/seal behavior. Preserve whole-Work/Candidate false precedence and hidden raw intent. Do not change immutable accepted choices, RuleB pin, rates/claims, money counters, projection producers, F25 parked files, future Candidate/Case controls or any unrelated owner.

Source recommendation is complete. Product source changes, ordinary existing controls and actual joined screen/acceptance receipts remain the builder's separate work; no runtime result is asserted here.
