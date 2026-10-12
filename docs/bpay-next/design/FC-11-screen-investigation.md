# FC-11 screen investigation — sequence 132

Investigation only, 12 October 2026. No product, pack, checklist, capture, test, database or hosted operation was performed. A4 approval is NOT_RUN until root supplies the actual implemented screens. Root authors product.

Read the full FC-11 card (`03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:1238–1266`), all ITEM/D01–D15 checklist rows, current `FC-11-investigation.md`, applicable repository AGENTS, the policy and 42-item checklist, inventory/index, and B9 specs/PNGs. The supplied three PNGs were individually viewed. D09 owns the single per-component Write off window; D08 owns its existing finite Worker mappings. Duplicate function/context rows do not authorize a second protocol or financial owner.

## Supplied screens and required appearance

Read-only pack root: `C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/cloudtms-backend/codex_outputs/claude-stage2-continuation-20260925/SCREEN_DESIGN_POLICY_20261010`.

| Screen | Exact supplied PNG | Exact supplied spec |
| --- | --- | --- |
| Amount form | `batches/B9/png/SCR-124_write_off_dark.png` | `batches/B9/specs/SCR-124_write_off.json` |
| ALL protected refusal | `batches/B9/png/SCR-124b_write_off_refused_all_dark.png` | `batches/B9/specs/SCR-124b_write_off_refused_all.json` |
| Applied result in Cases | `batches/B9/png/SCR-124c_write_off_result_dark.png` | `batches/B9/specs/SCR-124c_write_off_result.json` |

These are the final FC-11/EX-35 mapping (`DESIGN_INDEX.md:139`, `SCREEN_INVENTORY.md:200`). EX-01's existing auth child is SCR-66: `batches/B6/png/SCR-66_reauth_password_dark.png`, its phone variant, `SCR-66b_reauth_code_wrong_dark.png` and `SCR-66c_reauth_password_reason_dark.png` (index:81). No B9 phone counterpart is supplied; actual 390px phone evidence is still required.

The Write off design uses the native small 600px frame, actual Case/Candidate subtitle, amber irreversible warning, Outstanding / Protected in a Draft / Can be written off facts, ALL or AMOUNT radio choice, conditional Amount, required Reason and destructive Write off action at the left. Cancel is safe. The refusal retains typed input and names protection. The result returns to the existing Cases host with a green dated applied result and current remaining/protected facts. A write-off occurrence date is permitted; it is not a pay date.

## Native seams and exact gaps

| Current source seam | Smallest owning change |
| --- | --- |
| `FE/js/bpay-next-case-list.js:87–104`: current More → optional `options.caseActions.writeOff`; child options preserve actor/current/frame and `onAccepted` refresh | Supply the actual native handler. Pass real Case/Candidate identity, then obtain fresh component facts; select the actual RECOVERY component, never a payout component or an arbitrary first row. Preserve parent filters/scroll/current row after Close. |
| `FE/js/main.js:13002–13024`: `openBankingNextCases()` has fresh native owner/epoch, actor and modal-prefix qualification | Reuse these options and the existing native window adapter. The qualified callback also needs to be callable from an actual outstanding recovery entry after payment, as D09 requires. A Cases-only entry does not close that second reachability requirement. Do not claim a future payment host journey from a callback sample. |
| `FE/js/main.js:20962+`: `confirmBankingFinanceCaseWriteOff()` reads legacy state, uses the retired finance endpoint, numeric seed formatting and toast | A NEXT branch must qualify native ownership before those legacy reads and route into the same native component window. Do not reuse the legacy mutation, infer zero from missing seed money, or show a toast. Legacy behavior outside NEXT is a separate preserved branch. |
| `FE/js/bpay-next-office.js:726–755,1032–1046`: financial/reauth allowlists and closed input/result schema lack WRITE_OFF | Add only the existing WRITE_OFF shape and receipt, not a new action/route. ALL has no amount key; AMOUNT requires its exact positive decimal string. Reason and original UUID are immutable once submitted. |
| `FE/js/bpay-next-office.js:1740+`: exported `openBpayNextReauth(options,intent)` | Extend the existing allowed action, exact preview and same current actor/owner/modal scope. Reuse this helper, its password/code handling, configured second-factor policy, safe cancellation and busy/error roles. Do not duplicate password/code inputs in the Case module. |
| `BE/broker/src/bpay-next-command.js:362–374` | Existing result validation requires `case_id === case_component_id`; FC-11's genuine Advance/Manual Debt RECOVERY component can differ. Replace only that old Work-specific restriction with the actual qualified component association while retaining original command, component, scope and terminal monetary invariants. |
| Existing `private.bpay_next_write_off_receipt_v1` (1647:8–22) and native history WRITE_OFF/COMMAND | The mutation receipt lacks a date, but `BE/broker/src/bpay-next-history.js:54` already carries the exact command's immutable request, terminal amounts/status and `completed_at_utc`. Root's chosen seam uses this existing COMMAND-scoped history read; no mutation/job receipt expansion is needed. Show a written-off date only for actual WRITTEN_OFF. The browser clock or a broad audit walk is not authority. |
| Worker and Office EX-01 preview decoders | Current non-PAYE-policy enum is CASH/DRAFT_GROSS; native WRITE_OFF still defaults to CASH in 1358. Root's chosen action-specific basis is SOURCE_DEBT in the existing scope/preview/decoders. Forgiveness is not cash paid. Preserve computed scope hash, exact intent and same-original accepted replay. |

Existing endpoints are `/api/banking/pay/next/preparation` for component reads, `/api/banking/pay/next/history-page` for exact COMMAND-scoped WRITE_OFF recovery, `/api/banking/pay/next/reauth-preview`, `/auth/reauth/start`, `/auth/reauth/verify`, and `/api/banking/pay/next/command` for original WRITE_OFF. `FE/js/bpay-next-payment-history.js` already mirrors the WRITE_OFF section fields and supplies its bounded `createClient()` read adapter. Native job is CASE_WRITE_OFF. No new endpoint, command kind, journal framework or auth mechanism is necessary.

## Factual values and recovery

`CASE_LIST_ROW` supplies actual Candidate name/reference, Case label/status, current counter facts and first protected run/channel. `CASE_COMPONENTS` supplies exact Case/Candidate/component/revision, instruction kind, direction, source channel, approved/funded/recovered/written-off source amounts and authoritative Outstanding. Use that server amount; do not calculate approved minus recovered in Office. For an Advance, only funded debt is eligible; an unfunded Advance is not a zero-valued write-off success. CREDIT and unsupported origin/tax/basis tuples receive a truthful unavailable/refusal state.

Correction after checking the actual current `11102026_2041_banking_pay_next_case_pause_v1.sql:21`: `bpay_next_case_pause_state_v1` already invokes the existing FUTURE_ONLY assertion with explicit RECOVERY purpose. The earlier proposed `write_off_state` property/helper is unnecessary and withdrawn. Existing CASE_COMPONENTS Outstanding plus `pause_state.protection_summary` and `first_protection` supply the required facts for the four positively qualified RECOVERY families. Native writers own eligibility and capacity. Do not turn a genuinely null amount into £0.00 or infer a capacity formula in the UI. Pending accepted bindings deliberately make both protected amount and capacity null. Show why the amount is unavailable and lock the unsafe next step.

`pause_state.protection_summary` and `first_protection` expose native protected count/amount and run/worker/channel/status. `Office.openCaseProtectedDraft()` (`3165+`) already navigates a qualified unconfirmed run to review or a confirmed DRAFT through CHANNEL_DRAFTS/CHANNEL_STATUS. Use only a real retained tuple. There is no provided human PB reference here; label the native channel Draft and omit invented PB-0412-P. Do not add a cancel action to this window. Protection can only be released by its native owning action, and the wording must not promise that cancellation immediately makes every Case eligible.

Existing CASE_AUDIT rows have actual `occurred_at_utc`, source/event identity and financial event facts, but its closed input is a full Case sorted page, not an exact write-off event point. Use the existing history WRITE_OFF row scoped to the retained original COMMAND for this result and timestamp, with exact Candidate/Case/component/command qualification. The audit remains an existing optional evidence entry, not a required detour to discover whether this command succeeded.

Preserve finite English for INPUT_PENDING, PROTECTED_CAPACITY, AMOUNT_EXCEEDS_BALANCE, COMPONENT_CHANGED, ORIGIN_UNBOUND and NO_BALANCE, plus the card's comparison-review refusal if emitted. Keep user input on refusal; a changed component needs refresh/re-authentication of new intent, not silent rebinding. REQUESTED means requested, never written off. Unknown transport must retain/replay identical original bytes/UUID before unlocking a new request. A genuine terminal result drives the existing parent refresh; a failed parent read retains the factual result and offers manual Refresh rather than fabricating current balances.

## Necessary design differences and controls

Record these differences before implementation and verify their actual pixels later:

* Reuse the existing scoped EX-01 child instead of the supplied inline Password/Emailed code fields. This is the card's required EX-01 helper and avoids a second credential/auth implementation. The Case form preserves its exact frozen intent beneath the child; cancel returns safely.
* Use only native Candidate/Case/channel/balance/protection/date facts. Omit illustrative PB references, counts, account details, pay dates, bank/funding controls and future FC-15 origin support when the owning DTO cannot prove them. Keep required outstanding/protection/capacity facts; an unavailable fact is explicit rather than replaced with invented money.
* Place any refusal before retained facts/tools, use one plain sentence with a next step, and maintain danger/alert versus success/status semantics. Do not reproduce the supplied lower error placement when it hides the result from a phone user.
* The dialog is a form, not a new sortable list. Existing parent Case sorts/counts remain native; SDP-34 exempts dialog facts/result lists. No fake arrows or new summary strip is needed.

Every visible control has one payroll purpose: ALL/AMOUNT selects the intended forgiveness; Amount supplies the exact partial amount only; Reason supplies mandatory audit justification; Write off starts the exact protected money action through reauth; Cancel/header Close backs out; manual Refresh re-reads stale/unknown current truth; Open Draft appears only for a real protected native tuple and locates the protection. Auth password/code/Send code/resend controls belong to existing EX-01. No Note-only save, duplicate footer Close, repeated parent detail, additional tab, fabricated action or new screen is necessary.

Use native dark tokens, warning/destructive classes, money formatting without arithmetic, DD/MM/YYYY occurrence dates, header Close only and 44px phone controls. New CSS starts `#modal.ctms-modern-modal`; no `!important`. Destructive action is left visually; safe Cancel is first in DOM/default focus. During disabled/busy states the reason is visible. Preserve actual parent context and useful existing caret/open pattern; never add a redundant expanded copy of the same row.

## Actual screenshot acceptance plan

Root should capture the actual native host with the smallest fixed responses, independently of business acceptance: valid AMOUNT with protection; ALL protected refusal with retained terms; successful partial result/date/remaining protection and updated Cases row; unprotected ALL; Manual Debt recovered balance; unfunded/no-balance refusal; pending protection/null amounts; stale component; initial/retained read failure and refresh recovery; submitting/requested/unknown same-original recovery. Capture any new EX-01 action preview/password/code/error/currentness presentation, and actual reachability from both required callers when their native host exists. Do not invent a payment host or mark a deferred join PASS.

Compare every final dark/device-light/390px phone PNG next to SCR-124/124b/124c (and SCR-66 for auth), with phone bottom/scroll evidence where controls leave the first viewport. Save all 42 policy rows, every visible difference and every control purpose under SCR-124; any unexplained difference blocks A4. Item 16 uses the actual `tools/policy.json` wording/format lists manually; pack generation is excluded. Owner item 25 and the `readerAdditions` mechanism of 26 are N/A; real values still require named native authority. Items 23/24 need actual captures. Joined data, real payment/reauth dispatch, long-list/load and business acceptance remain NOT_RUN in this investigation.

## Saved-source checkpoint

SHA-256 at investigation read (these are source evidence, not implementation approval):

| Source | SHA-256 |
| --- | --- |
| FE Office | `34804A80191481FCC010D7598E734B2345EF802C19E6FE9CE99D0C6E9FA191E6` |
| FE Case list | `81762F7BFB98263ACB56B0D1AD191E925DFD703493EABABC1E521AA4CA0A0647` |
| FE shared Case preview | `DAC5DBF44B4DCBBD9825412239D312750409CF63D0A9658965093F6DF6837226` |
| FE main | `69D84496624EF0F3AEBFB204E1E81CD7EBD824A793BC130A8379D4E7A0992F5B` |
| BE command | `F18B320E8AB3111B87686F94122F39C5C6747B149E0A062BEC500FE5BC9B6F4A` |
| Pack policy | `8BC212B174469236F2B46DBF70EADC3664A946636458321CD3C77B7146323DE5` |
| Pack manual policy JSON | `45036D841D4568AA2D4D7ADF4F063D6D17B70288E289EE0B285068797A87369D` |
