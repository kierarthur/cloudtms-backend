# WB-29 Ready chosen DTO/presenter — independent source review

13 October 2026. **SOURCE PASS for the reviewed Worker/Office diff.** No essential source defect found. This receipt covers the closed DTO additions and existing presenters against native 0132 output; it is not a complete SQL-owner, pixel, runtime or whole WB-29 verdict. Reviewer made no product edit, SQL/RPC, business/hosted call, fixture, test or screen action. All app, screen/pixel and joined acceptance is **NOT_RUN** here.

## Reviewed current bytes

| File | SHA256 |
|---|---|
| BE `broker/src/bpay-next-workbench.js` | `03E3638C2ACDC449FC7553A18AED2690CF626A65C1D87EEBA62031EB5CB8577E` |
| FE `js/bpay-next-workbench.js` | `9660C16F124CF082B23437A3E881409CA9FFBAD6E62F3480B72DE682E3294A79` |
| Native output `supabase/repeatable/13102026_0132_banking_pay_next_ready_chosen_member_reader_v1.sql` | `FBB3936C4D74A6AB8D2C58ECA5336811E8B63331369E9898B55752B10E305968` |

The saved product diffs against HEAD were read, alongside the approved reader join and DTO/presenter map. BE has only the two output-map assignments and two kind-specific guard extensions. FE has the matching assignments/guards, the one leaf-money branch, the component-heading choice, the existing group badge and authorised-summary copy. No command, request normalizer, choice-store mutation, context/cursor shape or transport endpoint is added by these diffs.

## Contract and native-output correspondence

Both files **405–407** add exactly three explicit nullable GROUP_LINES fields (`chosen_component_ready_amount`, `chosen_leaf_amount`, `rate_set_pay_channel`) and one nullable CANDIDATE_GROUPS field (`rate_set_pay_channel`). Existing decimal-string and PAYE/UMBRELLA channel types are reused. The existing exact key validation remains: all properties must be present, including explicit nulls; arbitrary quote transport is not accepted. Envelope/request/page bounds are unchanged.

Both member guards **620–630** require the chosen tuple all-null or all-non-null. A present tuple is limited to known, selectable, unselected, taxable READY PAYMENT AVAILABLE/ZERO Work component/segment rows, with actual Work/revision/line IDs, known component Ready amount, CURRENT progress and exact target channel; held/snoozed rows are refused. These checks agree with the native strict facade's original leaf qualification (current 0113:85–101), which 0132 calls at **797–798**. Existing component-knownness, shift identity, Source/physical provenance, selection-basis and group-key guards remain intact.

Native **0132:759–829** resets all three additions to null per row and fills them only for the eligible output branch and qualified saved quote. It rejoins actual item/group/Work/revision/approved-line identity at **764–779**; forms bounded native full component membership and calls the common facade once per cached component at **780–806**. Known component Ready money is required at **808–811**. Leaf money is the whole quote for an unsegmented component, exact zero for a zero-included leaf, or the native prefix difference joined to the actual segment ID/key at **812–823**. The native SQL output is decimal text; Office does not recreate those calculations. **828–829** always packs the same three fields, including nulls. The response packing checks remain after enrichment at **831–837**, with selection-memory enrichment retaining the existing Source row fields.

The paired group guards **640–647** permit a non-null channel only for a known-money, complete, clean CURRENT READY WORK group matching its target. Native **0132:722–758** supplies that value only after actual current/applied Work, Candidate/Target/Umbrella/VAT, metadata/FVR and date qualification plus an exact compatible ACTIVE decision on genuine eligible taxable unselected Ready membership. Case and other-section output resets it to null. The existing accepted tuple and monetary-knownness checks remain. No rate-set claim is derived from Target alone.

## Presenter and Source-boundary findings

FE **1269** preserves manual-unavailable, selected captured Source, unknown and RESERVED precedence. Only the unselected READY leaf branch changes to `chosen_leaf_amount ?? included_source_ex_vat`; a genuine zero does not disappear through truthiness. Waiting/blocked/action, selected, reserved and frozen values retain their existing owners. Existing attributed-shift display uses that helper, while the genuine aggregate-without-attributed-shift path still shows no invented per-shift amount.

FE **1305** chooses a qualified member from the current bounded exact component group via `members.find`, then renders its server component Ready amount. This closes the documented `first`-member pitfall when the first portion is WAITING/RESERVED. It neither sums visible leaves nor calculates a conversion/delta. Separate waiting and in-Draft component values remain unchanged, as do the original complete-from-start conditions for the single-shift shortcut.

FE **1530** adds only the two literal badge choices, “Umbrella rate set” / “PAYE rate set,” using an existing native badge class and the closed native channel. Group money at **1531** still comes from server `group_amount`, with unchanged Case estimate/outstanding/due precedence. Whole-list Amount sorting/cursor validation still consumes that same native group amount; Candidate sorting retains its literal Source summary. There is no browser re-sort after enrichment.

The summary-basis ambiguity identified in the design map is closed in saved source: main and Candidate strips now say **“Ready as authorised”** at **854 / 1515**; Ready Candidate desktop/phone sort and cell labels say **“Amount ex VAT (as authorised)”** at **1075 / 1096 / 1100**; the filter card now says **“ready, as authorised”** at **940**. Their actual native HEADER/CANDIDATE amounts are unchanged. Separate NET additions and estimate/Draft facts are retained. Unresolved ACTION rows retain their PAYE/Umbrella “as authorised” qualifier at **1184 / 1533**. These labels do not invent a chosen summary or require it to equal the visible chosen group amounts. Actual layout/wording legibility remains a separate pixel review.

Snooze uses `amount(member)` only as an informational subject label; its native seed, expected identities/basis and command journal are unchanged. The diff leaves Work/component choice mutations using original revision/native selection basis and captured request identity, not the new money fields. No chosen output enters a financial command, no current/frozen offer is repriced, and no new query, automatic retry or typing-time request is introduced.

## Boundary

No open essential finding remains for the byte hashes above. This SOURCE PASS does not certify the independent full 0132 SQL preservation review, Root's compile/gate receipts, screenshots, performance or business first use. Those remain separately attributed and all reviewer runtime/pixel checks remain NOT_RUN.

Final concurrent-byte amendment: FE changed from the initially inspected `22868E96…` to the current hash above only within the already reviewed badge presentation: its native tone is now `candidate-office-badge--info` rather than `--neutral`. The latest diff/DTO guards, amount branches and copy were reread. That native modifier already exists at `css/candidate-office-v1.css:104`; no new badge styling or behavior was introduced by this change. SOURCE PASS remains; actual colour/layout acceptance is still NOT_RUN here.
