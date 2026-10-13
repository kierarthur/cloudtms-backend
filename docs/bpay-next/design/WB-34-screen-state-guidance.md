# WB-34 supplied-screen and native-state guidance

13 October 2026, sequence 146. Independent read-only guidance for sole assigned D06, against full card `02_WORKBENCH.md:1241–1260`, shared W12–W14, R2-F03/F04 and current BD176 boundary/investigation. This is guidance before product review, **not SOURCE completion, A4 approval, runtime, timing or joined business acceptance**. Root alone edits product. The reviewer viewed original local PNGs, read matching specs and current saved source, and wrote only this document. No pack copy/edit/spec generation, render, SQL/RPC/database/business call, fixture, network or test was performed.

## Original supplied designs actually viewed

Pack root: `C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/cloudtms-backend/codex_outputs/claude-stage2-continuation-20260925/SCREEN_DESIGN_POLICY_20261010`. DESIGN_INDEX row 43 maps SCR-29 to all four B2 pictures below. Row 30 maps parent-state SCR-16 to root T01/T04b/T04e; inventory also names T04f. Matching B2/root JSON and original policy were read without changing them.

| Original PNG, all individually viewed | SHA-256 |
| --- | --- |
| batches/B2/png/SCR-29_rate_save_pending_dark.png | D967B9F4A3ECC3D0903B214F11D6A3FF3A9BA69ACE1E571D62F93F2FE512E19F |
| batches/B2/png/SCR-29b_rate_save_refused_changed_dark.png | 902D7B4A937E0FD954D60DB924E22EA8012E8F1263D95E51D8198548F9008D28 |
| batches/B2/png/SCR-29c_rate_save_failed_dark.png | 1D68EB93697F8D1BE9BF5FF7C6B8CC083D0A56500F4E9D171B802016816C4A5F |
| batches/B2/png/SCR-21b_apply_rates_result_dark.png | BA504BFEB2326001FD77D8BEFDA317794713BCB5D12181F3281FB16EFF8F840E |
| png/T01_ready_list_dark.png | BA3D7A1D0BECD5B0C7209AC75E538958D87807A68B7CAC33482DF11B629D7B96 |
| png/T01_ready_list_devicelight.png | BA3D7A1D0BECD5B0C7209AC75E538958D87807A68B7CAC33482DF11B629D7B96 |
| png/T04b_action_required_dark.png | 67AEE0E3241F997580E551E54D7259BAE8E719CCC1A10D7C62685C28AC394A93 |
| png/T04e_loading_dark.png | 91536C7EA4AF5F5F65EE3A8A691205D45FA022E2CB34C88BBADEB4E3173F42E6 |
| png/T04f_message_patterns_dark.png | D593DAE2AF8DD9D1BA4D49EF1CFA0D561860D4A35F091AAE88FF572F6EF9A075 |

## Required state and close behavior

The card's successful-save path returns to the **same native parent**, retaining its tab, filter, sort, scroll anchor, unrelated expanded evidence and local unticks. Once the original accepted receipt has been durably handed to that parent and truthful pending state has been adopted, close the SAVE editor through its existing owned frame. Completion runs in the parent; do not leave a permanent Processing result window or wait for Ready before returning. A tiny accepted result may be transient, but it cannot become a mandatory Close, Refresh or reopen detour. If parent handoff cannot safely complete, keep the accepted child explicitly pending with the exact receipt retained and a read-only Check progress action; do not claim this fallback alone fulfills D06.

By contrast, refused/stale/unknown saves **retain** the editor, literal typed £18 (or its exact six-place rate string), Reason, linked choice and expanded evidence. A definitive stale refusal must show changed native evidence without silently replacing input or retrying a newer offer. This split, not unconditional close or unconditional retention, satisfies the full card and the supplied pictures.

| State | Native layout/copy/control guidance | Required authority and transition |
| --- | --- | --- |
| Initial read | Existing native frame, genuine subject header; one loading line and placeholders, figures —. Apply disabled with visible reason. | Existing W1 context only; no financial call or live-price fallback. |
| Editing | Keep actual rate/amount fields, quiet details and native Reason/linked consent. To apply/Suggested/Saved distinctions remain factual. | Typing calls nothing; preserve original exact context/owner pairs. |
| Known stale/refused | Danger notice at top: e.g. Rates not saved; check the changed figures. Keep the typed £18. Mark only actual changed evidence with Changed warning; show original/current native operands separately. | Original stale request is refused. Fresh evidence is read-only and bounded. An explicit staff review establishes a new offer before a new save; no newer counter/UUID auto-retry. |
| Definite rollback / save failure | Inline plain refusal with retained values. Try again is next to that refusal when valid. | Distinguish proven rollback/absence from unknown. Respect existing controller settlement/retry qualification; do not invent a generic retry route. |
| Lost/malformed/unknown reply | The result has not been confirmed; Check result is the sole primary. Retain original body/UUID, old scope and typed values. | Recover before any fresh context read. Try again becomes available only when original readback proves absence, and repeats identical original bytes. |
| Accepted pending | In original parent, affected rows show info Updating…; a brief success line may say Rates saved; this payment is being updated. Remove only stale executable resolution actions. Keep separately qualified original amount until native completion; do not derive new money. | Accepted receipt is mutation evidence, not Ready. Retain exact published revision/component mapping and continuation after command journal settles; close SAVE child after safe pending handoff. |
| Completed affected adoption | Adopt the native affected rows, sections, counts, totals and applicable estimate together at one coherent context. Show actual saved badge/value or actual Waiting/Blocked/Action state. | Exact published tuple and complete native member state only; no missing-progress-row ⇒ Ready shortcut. No manual Refresh/reopen prerequisite. |
| Accepted follow-up read failure | Keep the parent/receipt pending, last figures visibly qualified/greyed, plain Could not refresh with read-only Try again/Check progress. | Never turn accepted SAVE into failed-save £0.00 or resubmit money. Retain exact failed cursor and original accepted identity. |
| Reopen | Native persisted decision for valid mapped published revision; recover unknown or accepted continuation first when retained. | An old based-on pointer is not a fresh current offer; do not silently substitute arbitrary current/history rows. |
| CLEAR completion | Preserve WB33's same-editor remapped future context/Ready result where its existing modal behavior requires it, then coherent outer-parent adoption. | Do not close the whole Candidate/pay-channel parent or undo the original WB33 actionable restoration to imitate SAVE. |

The supplied pending picture says Moves to Ready when updated for its illustrated eligible rows. That phrase must be omitted or qualified for general current work: a pay hold/query/snooze or changed native eligibility can produce another state. Only completed reader facts justify Ready. Updating…/Processing describes accepted work still being materialised, not payment execution. No queue-duration or two-second Ready promise.

## Actual supplied layout and necessary omissions

SCR29b/c show one native wide rate editor: title and Candidate/TMS/Timesheet/client subtitle; top inline danger notice, then existing channel warning; one universal table; linked consent/reason below; native footer with one primary. Fixed Standard order Day/Night/Sat/Sun/Bank holiday is retained. Numeric facts are right aligned/tabular; 13px table radius, 10px controls, native dark tokens and flat grouping. Phone becomes readable cards with 44px controls and no sideways scroll. Native amount/component and channel editors retain their existing smaller/own layouts; WB34 adds state/evidence treatment rather than duplicating the Standard-rate form.

The changed screenshot shows Hours changed 22.50→23.00, typed 21.10 and actual current/suggested/charge/margin fields. These are illustrations. Display only actual supplied native operands and changed fields; absent attribution/rates/charge/margin remain —. Do not manufacture Contract prices, conversion percentages, pro-rata money, borrower facts or current figures merely to match the picture. Preserve SOURCE/original approved, TARGET suggestion/chosen amount, genuine charge, residual and available schedule distinctions. The UK internal valuation date stays hidden before execution.

SCR21b and SCR29 pending illustrate **3 of 4 Timesheets saved**, one refused. The actual native RESOLUTION_SAVE is atomic for its disclosed request. Required omission: no partial SAVE table/count, no fake per-Work success/refusal, no smaller automatic resave and no newly split financial operation to reproduce that picture. Render all accepted receipt Works as accepted/pending, or the entire failed request as not saved with retained inputs. Per-Work completion states may differ later, but those are preparation/projection outcomes, not partial SAVE. WB28's separately owned paged partial CLEAR is not evidence that SAVE is partial. Record this concrete difference in BUILD_LOG and the eventual A4 receipt.

T01/T04b/T04e control **parent** behavior: real native server totals/counts, same selection/scope, per-row Updating…/Failed rather than a whole-window rebuild, failed read retains last figures greyed instead of zero, loading initial figures are —. Do not paste their mock Candidates/amounts/main chrome into a result child. T04f controls inline refusal/result/busy shapes and two-button confirmations. No toast, extra tab tier, footer Close duplicate or new overlay.

Sorting remains the real existing whole-list server sort for touched parent data headers. Standard fixed business-order rows, expanded evidence children, dialogs and result lists are SDP34 exemptions; do not add a fake client sort or new sorting service for changed-evidence cards. One primary next action per native window, destructive left and safe confirmation first, dark-only even on device-light. Use existing exact-input/formatted-money opt-outs so native presentation decorators cannot round six-place manual rates. Error copy follows the actual policy/owner literal rules, with no technical code, revision, owner, phase, cursor, UUID or pre-execution pay date shown.

## Precise existing native seams

| Existing source seam | Minimal WB34 integration responsibility |
| --- | --- |
| `js/bpay-next-rates.js:31 acceptReceipt`; SAVE/recover/retry converge there | Replace result-only broad invalidation with one accepted-publication handoff. Current render's perpetual Processing table is an interim state, not completed adoption. Preserve inputRates/originalRates/reason/includeLinked/expanded when refused. |
| `js/bpay-next-component-choice.js:56 accept`, `:76/78/79` | Same exact accepted lifecycle for Additional/Adjustment/manual amount; retain mode/value/reason/linked, not just Standard rates. Current stale Refresh cannot simply discard £18 and close before changed evidence review. |
| `js/bpay-next-pay-channel.js:99 accepted`, `:100–102 dispatch/recover/retry` | Use same receipt handoff; retained row fields/Reason and finite finance pane remain. Current generic onChanged invalidation alone does not produce prompt same-parent completed facts. |
| `js/bpay-next-clear-choice.js:87–122 restore`, and three parents' `onRestored` | Reuse exact original accepted COMPONENTS remap + W1 + GROUP_LINES state/continuation. Keep existing own-editor restoration and original request/accepted cache; broader WB34 adoption must not invalidate the caller before this completes. |
| `js/bpay-next-office.js:3078–3086` current editor/Candidate callbacks | Present callbacks invalidate issueLists/workbenchShell. Stage coherent affected-parent state and lifetime-safe pending handoff here, rather than invalidating its own editor current() first. |
| `js/bpay-next-workbench.js:1210/1254` issue caller and `:1475/1571` Candidate caller | Retain real actor/owner/agency/epoch/navigation/frame identity while legitimate own accepted transition is adopted. Do not bypass guards by making current() always true. Unrelated navigation/owner/actor change cancels visual/focus side effects. |
| `js/bpay-next-workbench.js:1140–1167,1224–1242,1580–1597` list/Candidate reads | Existing refresh may reset stale cursors to page one or clear child caches; do not use it as atomic adoption. Preserve actual native anchor, retained bounded pages, detail objects and same local choice store; stage all aliases plus headers before one replacement. |
| `js/bpay-next-resolution.js:428–489 createResolutionCommandController` | Original 32KiB body/local journal is sole financial retry owner. It stores before dispatch, readbacks first, retries only proved absent and settles accepted receipt. Keep it unchanged; accepted continuation is bounded screen/parent metadata, not a second financial authority. |
| `js/bpay-next-ui.js:32–74 createStandardWindowAdapter` | Reuse actual stack/frame. Adapter preserves owned content/footer, confirmation initial safe focus, exact prefix guard after awaited beforeClose, and close() Promise<boolean>. Await true close, then recheck caller/actor/owner/current parent before focus restoration. |

Current rates onDismiss restores previous DOM focus if connected/actor/owner match, while Component/Channel also guard caller current. A stale/removed launch button needs a deliberate nearest valid control/row or parent heading **in the same still-current parent**, not focus into a newly opened window. Closing the child must not dismiss the Candidate/issue parent, change tab/filter/sort, retick unrelated work, remove unsaved unrelated input or navigate to page one. Completion after child dismissal belongs to the captured original parent's bounded lifetime, not the closed editor's current() predicate. Stop timers/listeners on final parent dismissal; retain original accepted metadata for legitimate reopen. Visibility-gated finite continuation is appropriate; whole-Candidate long polling is not.

## Changed-evidence transport and consent boundary

Current original closed W2 faults at `resolution.js:408–427` contain error flags only; no changed-evidence value may be inferred from them. If the root implements the card-required narrow native attachment at the existing wrapper, add only its exact typed existing-reader schema to the original fault decoder and mirror. Validate original actor/Candidate/Work/component identity, native current context, strict decimal/null fields and existing response budgets; malformed/unavailable evidence stays unavailable without turning typed £18 into a fresh offer. No additional route, generic fault framework or browser-calculated financial facts.

Alternatively a bounded fresh read may display authoritative changed evidence, but it must not silently become mutation context or force full-parent Refresh. Preserve original typed state independently of fresh read-only comparison. The staff member explicitly reviews the changed evidence and chosen retained input before a new request/confirmation; original unknown outcome is reconciled first. Actual effect/charge/available facts use current native outputs, with no financial derivation. This note does not approve an unimplemented attachment.

Every visible new control must have one payroll purpose: read-only changed details explain refusal; explicit Review again/new review consents to changed evidence; Check result reconciles original unknown; Try again repeats only the permissible original absent request or read; native Close backs out; accepted pending needs no extra user click on the normal path. Keep existing zero/manual-reason/linked confirmation safeguards. Never duplicate Apply and Retry as two primary financial actions on the same state.

## Source snapshot and delivery limit

| Read source | SHA-256 |
| --- | --- |
| js/bpay-next-rates.js | E2F1BD104E4B4013749E92CB89BC32FA2E3F3B5DC3766CB3B0D800BAE6E10132 |
| js/bpay-next-component-choice.js | 23B5D5E56B7ABF1A84DE1BC59A6DBE4307B57AB799565E401C62440AF9DC10A2 |
| js/bpay-next-pay-channel.js | 38CC12BA6228F0DCD6CC331C8CD04E751ADC49975238AC7B4574CD8D61ED6A16 |
| js/bpay-next-clear-choice.js | 337A85FE4E3D044FA14EE054E5004731D67E8515721866054AE748EE2856566B |
| js/bpay-next-workbench.js | 83BD53D7CBD721D9A098DE705748EEEC6068C85CE618B78BF7C94FA6C9F8F1CD |
| js/bpay-next-office.js | 173F02F5FA0CA3B235443BBDCA22B5BAD616AF33E7AA84090BA246C75BF18BBB |
| js/bpay-next-resolution.js | 1C01BC2A743D1A6D4489B80802AB499334294F96CD240FF57C40A5F8542E2F98 |
| js/bpay-next-ui.js | F535E7E75579428CA7B97B57D406CCAEADAEBBF0FE15E5935574F435B772248D |
| WB-34-build-boundary.md at guidance read | D3DD0B9F1BAD40B260C6947B5D2BB403B9D58332BD386CDF40140565502FA9FE |
| WB-34-current-investigation.md | 7C51592EA483900C7CAABFF16B3C9FD3C998487CDDCE2D48B23C80AFE0C8ED55 |

This note supplies concrete UI/state/currentness guidance, not approval of new code or reader-anchor/changed-evidence designs. Root must source-review the actual implementation and then produce native current dark/device-light/390px captures for all touched states, including retained £18/refusal evidence, unknown recovery, accepted pending, coherent completion and reopen. The later full42 A4 comparison must explain every actual visible difference, especially omission of illustrated partial SAVE. Two-second affected usability remains a later measured actual modal/Worker/RPC goal; background Ready completion is separate and presently not measured.

