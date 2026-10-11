# DR-10 Office source review

Independent read-only review of the current saved Office, Workbench and scoped CSS against DR-10 card/checklist, BD111 design, Rule B/W13 and owner A4. No product edits, tests, database, hosted calls, authentication submission or business actions. Joined application acceptance is **NOT_RUN**.

## Current checkpoint

**Final independent Office/Workbench/CSS source PASS. No essential source finding remains.** Current Office SHA256 9791DBE4268ECDB63F0DE7DACAF0DCE929EAD441B6CEDD0BAB181B450F2E6B1F, Workbench D36FAC111CD39ABCDA1AA5C694FDFC3BBD238270003B796A31D22B92E97B8909, CSS 7029EEAC8A49DCC025A4F036840543D2F1EC495CEB15436A02EF192C552F27EB. Source positions below are in the frontend worktree. Historical first correction checkpoint was Office BF9738E7… / CSS 2A2EADB1…; historical phone-error checkpoint was Office66E585D2…. Neither is the current receipt.

## Findings and correction history

1. Original new guard error paragraph started empty, so existing native cleanup (`js/modal-modernisation.js:1289–1290`) removed it before later preview/auth failures could populate it. Current `js/bpay-next-office.js:1143` adds the existing `data-next-live-copy` marker. This precise defect is closed by source inspection; no new window mechanism.
2. Original stale-count catch recognized only `BPAY_NEXT_CHOICE_CONTEXT_CHANGED`, while the existing native count reader throws `BPAY_NEXT_WORKBENCH_CURSOR_STALE` for actual local revision/coherence changes (`js/bpay-next-workbench.js:1121`). Current Office `:1291` admits exactly those two codes to the plain Refresh refusal. Existing WorkbenchReadError assigns both code and serverCode (`:641`). This precise defect is closed; no automatic scope retry was added.

## Retained source behavior

- Closed GUARD page validates original run, original namespace (agency/actor/epoch/date/filter), Workbench/adoption revisions, bounded native existing-Draft facts, current channel counts and decision/channel consistency (`js/bpay-next-office.js:293–301,575–589`). Labels do not expose raw run UUIDs or invented amounts.
- New PAYE_GUARD_DECISION request retains original run/command/full descriptor, closes the three admitted decisions, requires the verified token for either PAYE option and forbids token/reason on Umbrella-only (`:411–430`). The compact EX-01 policy intent binds the original identity/context/reason; it certifies no money.
- Existing journal version, original CREATE receipt and every previous pending mutation remain readable. New pending decisions must match the original identity/descriptor and CREATE phase; the decision receipt is reduced only to the original CREATE receipt for subsequent original phases (`:730–755,809,837–850`). Unknown outcomes retry stored original bytes before a new GUARD read.
- Fresh flow reads GUARD before original Create, checks unchanged native context/selection before and after both guard read and auth, then either invokes the decision or original CREATE. Cancel retires only an unaccepted, nonpending CREATE journal (`:845–847,1718–1731`). Accepted sequencing/pages/Seal/readback/acknowledgement retain original flow. No browser financial step or newer-price retry.
- Post-Case GUARD phase remains the existing sequencing seam; the frontend addition is the pre-Create policy read/decision. Final acceptance depends on the named SQL seal/guard owner, reviewed separately; this note does not claim a second post-Case GUI decision.
- Paid guard presents required reason and unticked explicit confirmation; unpaid hard-block presents only Umbrella-only plus Cancel/Close (`:1115–1168`). EX-01 cancellation obtains no accepted decision and preserves the first guard window.
- The existing create-count stale path offers explicit Refresh, keeps ticks, and retires only the still-unaccepted original journal when appropriate (`:1278–1295,1754`). Normal permission, actor/agency/epoch, gate and current-scope guards remain.

Visual evidence, complete 42-item policy review and remaining visual findings are saved separately in `docs/bpay-next/screen-designs/SCR-39/review.md`. No runtime/item completion verdict is conferred.

## Subsequent exact corrections and finding closure

3. Exact stale auth/preview codes were swallowed inside the guard with no Refresh action. Current guard Promise rejects those two closed native codes after safe close (`:1131–1132,1164–1165`); original outer unaccepted CREATE branch offers Refresh and retains the same journal until user explicitly refreshes (`:1759+`). No automatic Create/new descriptor retry.
4. Missing reason/tick and actual zero-Umbrella disabled state now expose plain footer hints (`:1147–1153`); generic preview refusal gives the one-sentence retry instruction (`:1167`).
5. Scoped CSS uses SDP23 prose card16/input10 and44px phone confirmation label. The prose16 versus policy.json card13/section16 ambiguity is explicitly recorded in the screen review; no new feature is inferred from it.
6. Preserved empty paragraph initially produced a visible empty danger box despite hidden. Current narrowly scoped `.bpay-next-paye-guard .ws-notice[hidden]` rule closes that defect; actual final24 desktop/phone non-error images show no empty box.
7. **CLOSED:** Historical `DR10-guard-error-phone-dark.png` (14:53 generation) hid the real retry sentence beneath the sticky footer. Current `:1168` reveals the populated result in the existing native modal body on the next animation frame, guarded against dismissal. Actual final14:56 phone error clearly displays the full sentence above the footer, preserving reason/tick and original choices; desktop/device-light remain readable. No new dialog/action/scroll system.

All24 final captures were individually viewed. The full42 policy receipt is33 PASS /9 N/A /0 FAIL with every visible departure/control explained. This source PASS is confined to named DR10 Office contracts and retained original submission/recovery flow; SQL/Worker authority is separately reviewed. No application, keyboard, token consumption, concurrent Create, financial, queue or hosted acceptance was executed.

