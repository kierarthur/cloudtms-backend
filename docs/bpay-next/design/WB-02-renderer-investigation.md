# WB-02 renderer and retained-owner investigation

Read-only on10 October2026. Read full WB-02 card02:120–152, all checklist objects D01–D09, WB-13's renderer contract and checklist D05, and master W10/sequence. Inspected saved BUILD-FE `js/bpay-next-office.js`, `js/bpay-next-payment-history.js` and the actual main.js label seams. No product edits, tests, fixtures, SQL/runtime, network or secret reads.

## Exact screen ownership

WB-02 explicitly assigns its screen wording to **the WB-13 renderer**. WB-13-D05 names `js/bpay-next-workbench.js`, which does not yet exist in BUILD-FE. Master sequence50 is WB-02,51 WB-06,56 WB-13. The current thin NEXT Office view consumes preparation, workers, frozen Draft details/status and history; it does not consume maintained Workbench issue rows. Neither this caller, payment-history.js nor main.js contains either withdrawal reason code. There is no current qualified NEXT Workbench renderer to amend for these codes.

Minimal truthful disposition now: retain the exact reason-code contract and the later WB-13-D05 requirement in the completion/source note. Do not build the Workbench screen, new reader/route or an unused parallel label helper early. The later renderer must consume the stored reason_code and escape the fixed wording, without deciding approval, paid state or money:

| Stored code | Exact display | Stored section |
| --- | --- | --- |
| APPROVAL_WITHDRAWN | Missing approval — approval withdrawn | BLOCKED |
| AWAITING_REAUTHORISATION | Paid — awaiting re-authorisation | ACTION |

The Action Required badge/footer includes the latter from the same maintained revision. WB-13, its WB-09 reader/route dependency and WB-44 adoption remain genuine later work/application acceptance. This note is not evidence that those screens exist.

One current documentation correction is warranted: `design/WB-08-workbench-projection.md:69` still gives APPROVAL_WITHDRAWN as merely 'Approval withdrawn'. The canonical reason wording should become the exact WB-02 phrase above; its existing AWAITING_REAUTHORISATION wording already matches. This is a reason-contract correction, not an early screen implementation.

Actual main.js `friendlyCodeLabel` at11080 humanizes arbitrary codes inside the existing recovery context; `reasonLabel` at342230 escapes `it.reason` in a separate audit/settled view. Neither is an actual NEXT Workbench reason_code renderer. Existing legacy Workbench/details renderers are also outside the current NEXT scope; adding these codes there would not satisfy WB-13.

## Precise retained Source/core acceptance and boundaries

- **Core1324 schema:** its `(approval_state='APPROVED') = (current_revision_id IS NOT NULL)` constraint already requires a withdrawn work's current pointer to be NULL, while its applied pointer and original position facts remain. No core schema edit is needed to express withdrawal. Last approved/realised figures come from exact applied native positions, not from a fabricated zero revision/current pointer.
- **`private.bpay_next_work_payable_state_v1(uuid)`**, final physical1430: its actual SQL STABLE coarse result is NOT_APPROVED / APPLY_PENDING / READY from approval state and current/applied pointers. Reuse unchanged; this is not the component classifier and cannot stand in for the WB02 projection branch.
- **`public.weekly_source_first_authorise_v1(uuid,uuid,text,uuid)`**, final physical2018: after the genuine successful outer Source authorisation it NEXT-gates stage_source_current, publish_source_staged_pair, then initial_tsfin_bind metadata, retaining the factual core reply/gate. This existing real tail needs no new renderer-driven rewrite or duplicated publication/projection ownership. Eventual applied-position projection belongs to its actual ordered owners.
- **`public.weekly_source_first_authorisation_withdraw_v1(uuid,uuid,text,uuid)`**, final physical0556: the genuine Source owner already changes `weekly_source_root_authorisations.withdrawn_at_utc` through its qualified exact-row path, preserves original factual guards, handles Source head supersession, durable receipt and audit, and suppresses legacy financial callbacks under NEXT. When the named withdrawal trigger adds its bounded projection refresh, the outer routine should be reused rather than adding a duplicate refresh/replacing its factual workflow. This source acceptance does not claim paid Source withdrawal is universally permitted: actual1580 currently retains the distinct exact-work financial-dependency refusal, which must be reconciled with the real Source contract rather than removed for a display label.
- **`private.bpay_next_source_withdrawal_guard_v1()`**, current1580: this is a named WB02 edit seam, not confirmed unchanged. It already NEXT-gates and locks Candidate control→exact work, validates SOURCE ownership, retains its financial dependency check, updates WITHDRAWN/current pointer NULL and financial_view_revision, and creates no revision/position/job/effect. A bounded refresh immediately after its genuine mutation is the narrow card-required support. Existing money/source checks remain separate from renderer work.
- **`private.bpay_next_ordinary_authorisation_trigger_v1()`**, current1530 after the WB36 selected successor chain: the actual withdrawal/projection support is named by WB02; re-authorisation does not justify building the position writer out of order.
- Master W10 explicitly orders **1430 position writer WB-06→CM-10**. Do not create an early apply_position_page or publish_staged_revision successor as part of this FE investigation. The WB02 checklist reference to the reused publication object is not authority to reorder that chain. WB06 immediately follows and owns final applied-pointer/position projection integration; any acceptance note must distinguish retained publication evidence from that later unbuilt hook.

WB02's shared work WAIT predicate and classifier should retain exact indexed position facts, stored-before versus actual-after WAIT transition and linked native collection Cases. No renderer determines money, debt or origin eligibility. Withdrawal after accepted Create is next-run information and must not mutate captured Draft facts, selections or holds. Rule-B existing selection authority remains; parked F25/producers/PIN/enroller are untouched.

## Source receipt

- FE office SHA256 `9a946927ad0329c5da6e8da550543ec0cce6d34ac6dfd6b9e0e188a21af6bab4`.
- FE payment-history `467a78cd820f61cb0ee0e461f02f78d6c06f468024e7121f629770351f074d4e`.
- FE main `3e651b6f4497a723487f80b0b452b3330b41bd1f0ba89b47aa2f7ed0afd29dae`.
- Core1324 `f38878c7b09e3fbac2d21acbf96c4e88bdac1d4c32abac7ee98d2672df7ba52c`.
- Native1430 `395ead5cf46a1399ef4859f056d89508b0862f9b0fda8dde92454107f17602fa`.
- Source1580 `90e1f9ad45ba1d64a9e02b009058da45841da2d0515b057eda29699349659ad1`.
- Foundation2018 `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b`.
- Source0556 `64ef7e6d04e1b652af722cb9dd050310fcd8e1cd249f59b9359428a310b70933`.

Verdict: renderer ownership is WB13-D05; no present genuine FE consumer gap can be fixed without building it early. Exact labels and later acceptance are retained. Existing core/coarse/Source factual wrappers can be reused with the qualifications above; named withdrawal projection support is still actual work, and WB06 position writer remains later.
