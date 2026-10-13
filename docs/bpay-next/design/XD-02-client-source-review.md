# XD-02 client source review

Independent read-only review, 13 October 2026, seq148. Scope: saved Office BIND_STORED_CREDIT input/receipt/routing, optional stored-Candidate controller metadata, confirmation/retry/readback and Candidate Cases join. Source only: no product, checklist or BUILD_LOG edit, test, SQL/RPC/business call, database, browser/render, network, fixture, rehearsal, Git mutation or deployment. Root alone implements and executes checks.

**SOURCE FINDINGS OPEN at this checkpoint.** The native owner/request/journal reuse is narrow and concrete. Two client completion/lifetime defects below must be resolved before source closure. A4, runtime and whole-card acceptance remain separate.

## Exact reviewed checkpoint

Office js/bpay-next-office.js SHA-256 30E7A1D8169513799DE226EBE69478EFE4FE4A192A777DD954C177330F8BBDD4. Candidate Cases js/bpay-next-case-list.js SHA-256 29885808E94926F64D132691211839A337E6722A4AF4025CD96EBC0A00505716. Earlier first-iteration Office 2008A4B9267C73908614C1BF40CC7BB802479CE6F1A9545FF26A15EDF5EE0B48 was also inspected; the later hash above includes the small copy/header/locked-state cleanup. Findings below remain present in the later saved bytes. Subsequent concurrent fixes require a new receipt, without deleting this history.

## Open findings

### XC02-01 — submission settling hidden leaves disabled confirmation

Office1155 disables Cancel and Adopt when submit begins. finally1158 closes the confirmation only if local(), but local1152 includes document.visibilityState!='hidden'. When the server request settles while the document is hidden, the close is skipped. On return the owned confirmation remains top with both controls disabled; parent resume is inactive and cannot continue readback. The request journal is preserved, so this is a client recovery/lifetime defect rather than evidence of duplicate admission.

Required narrow correction: retain a settled close/resume action and complete it when the original confirmation becomes visible/current, or safely settle the owned confirmation without treating document visibility as the lifetime check. Use the same actor/owner/parent/frame guards. Do not resend the mutation, generate another UUID or infer its outcome. Keep hidden reads/paints guarded.

### XC02-02 — parent refresh failure still clears accepted receipt

Office1171 awaits onStoredCreditAdopted, then1173 clears stored_credit and states that the Case now shows in the Candidate's cases. The actual entry161 passes the existing accepted callback. Candidate Cases accepted51 awaits refresh and child refreshes, but root read63–74 and the child load catch their errors and resolve. Awaiting the callback therefore does not establish that the affected parent/Candidate refresh succeeded. A failed refresh still clears the retained accepted pointer and displays completed copy.

The XD-02-journal-investigation.md step8 expressly retains the accepted pointer if acknowledgement or parent refresh fails. The card requires the new Case visible in current Cases. Require a confirmed affected refresh outcome in the XD02 join, while preserving unrelated existing callbacks. On failure keep the accepted receipt and allow read-only Check progress. Native acceptance must remain final; no further Adopt or new command is appropriate.

## Verified narrow source behavior

Office INPUT243 and RECEIPT273 add only the already installed BIND_STORED_CREDIT command_id/legacy_case_id and five-field accepted receipt. financial818 selects the existing command route. requestBody remains closed; receipt872 additionally enforces case_id=command_id. No client amount, bank/hash/version, tax, Candidate substitute or RPC selector is posted. The exact native binder1827 admits only the original genuine Umbrella/non-taxable/one-off credit now PAYE and invokes the existing CREDIT/MANUAL_CREDIT/NON_TAXABLE Case-create owner; it keeps original bank origin independently. No database or Worker owner change is needed by these client additions.

Optional storedCreditScope uses original actor plus actual agency/Candidate in the stable existing controller journal qualifier. owner_epoch is persisted and compared with the current supplied scope, not embedded in a changing lookup key. On reopening, the saved journal is read before any live list. The optional version7 fields do not alter ordinary version7 journal keys/shapes. Foreign run/worker, different scope/Candidate or non-BIND pending action fail closed. Accepted metadata validates the original canonical request bytes, five-field receipt and command/case identity and cannot coexist with pending unknown bytes.

Original mutate saves pending before network. Storage failure prevents dispatch. Original dispatch(false) may clear a definite initial refusal, while unknown timeout/malformed/lost receipt retains exact bytes. Explicit retry uses dispatch(true), keeping the original UUID/legacy IDs and retaining unknown identity after later refusal/auth failure. There is no automatic financial retry. A known accepted receipt is saved with its request bytes before the fresh stored list/readback. Accepted or unknown state locks another Adopt; closing the list does not discard its journal.

The native confirmation uses the actual retained row, amount, Not taxable, whole beneficiary and masked last four. Opening/selection/cancel sends nothing; confirmation generates one UUID. Native header/modal adapter and accessible status/alert controls are reused. The optional binding scope is supplied by the actual existing Case List native context and original Candidate Cases frame.

Readback is the existing CASE_COMPONENTS exact Candidate/Case point with null after and limit1. Receipt phase ACCEPTED_PENDING_CASE is never treated as materialisation. Zero rows retains accepted metadata and pending copy; failed/mismatched read retains the accepted receipt and honest inability-to-confirm copy. Returned rows must match original case/Candidate, CREDIT and NON_TAXABLE. The native binder creates MANUAL_CREDIT. It is correct not to require new component source channel UMBRELLA: original Umbrella origin is frozen separately while admission captures the current PAYE Candidate. No browser conversion, balance recomputation or post-Draft live fallback is added.

The previously reviewed XD01 whole-list native sorting, hidden list-reply retention, empty raw continuation and cursor/error guards remain in the inspected source. No A4 or runtime execution was performed by this reviewer. Findings were sent to Root immediately; Root owns all fixes and required checks.

## Closure receipt — saved fixes, 13 October 2026

**SOURCE PASS for the reviewed XD02 client additions. XC02-01 and XC02-02 are CLOSED at the independently reviewed fix checkpoint below.** Original findings remain as history. No new concrete source regression was found in the fix delta. This is source only: no tests, native browser/A4, database/business behavior, installed provenance, latency or whole-card acceptance was independently proved.

XC02-01 closes at Office1152/1158. localOwned retains the original actor/owner/parent lifetime plus owned alive, top confirmation frame; local separately requires document visibility for consent. Confirmation finally closes under localOwned, so a request settling hidden cannot leave its disabled confirmation top merely because the document is hidden. Stored-list parent paint/read and reconciliation remain guarded by visible. Acceptance is retained in the original journal and resumed through existing mount/visibility/current-frame guards, without a financial resend or another UUID.

reconcileAdoption1182–1185 performs only the existing sorted list refresh followed by the exact native Case point. It requires visible, list not busy, no checking/adoptionRefreshing, and the retained accepted command. The list read must succeed under the same frame before its command is marked refreshed. Hidden pending replies/errors do not clear acceptance. Resume1227 and list finally1219 trigger deferred reconciliation after hidden return/reopen or acceptance arriving during an in-flight list read. Check progress uses this read-only path; finally clears adoptionRefreshing and repaints, so the control re-enables after failure/incomplete creation. Unknown outcome still offers only explicit original-byte retry.

XC02-02 closes at Candidate Cases161 and Office1171–1174. The new local storedCreditAdopted wrapper checks the original Candidate/frame, awaits the unchanged generic accepted callback, then returns true only with the current owned frame, no root Cases error, no child error line, child not busy and a currentFacts entry for the exact accepted Case belonging to the original Candidate. Swallowed refresh errors therefore cannot be mistaken for successful acknowledgement. Unrelated existing accepted callbacks remain unchanged.

Office now requires acknowledged===true after rechecking visibility and exact retained command before finishStoredCredit. False or missing acknowledgement retains the authoritative accepted receipt and read-only Check progress copy. The exact native Case/Candidate/CREDIT/NON_TAXABLE point is still required first. Zero/failed Case reads retain accepted state. No path converts acceptance into a fresh Adopt, replacement command, or current money/account request.

Original closed input/receipt/financial routing, optional actor/agency/Candidate version7 namespace and persisted owner_epoch checks, pending-before-dispatch exact bytes and same-command explicit retry remain. XD01 sorting/raw-empty source qualification is unchanged. No new money, producer, command, route, database or Worker changes are claimed.

| Independently reviewed fix checkpoint | SHA-256 |
| --- | --- |
| FE js/bpay-next-office.js | D1CB94DA22123D7F4F1AF665D5964E9F8B1639E9BF7D5EE972E8C10D2DD37B7A |
| FE js/bpay-next-case-list.js | 49E8A219E79A9E8F72851065CE3CC31CD768E0A38A79093A2711BFA38931D8AA |

The append guard then observed concurrent saved Office SHA-256 97F4E23C5EBB0779A220CA410204F4713DBCDD9BBD96BF5D29CF8D17D0412BCF. Its affected confirmation/reconcile/acknowledgement branches and optional stored-scope controller locations were reread and retain the same fixes; Case List hash was unchanged. The closure above is pinned to the fully inspected fix checkpoint and does not silently certify unrelated concurrent changes elsewhere in the whole Office file.

Reviewer actions remain source reads and this review-document append only. No test, SQL/RPC, database, business call, browser/render, network, product/Git mutation or deployment was performed. Root owns all required execution and joined A4 checks.

## Final settled-source pin — 13 October 2026

**SOURCE PASS remains; all reported XD02 source findings XC02-01/02 are closed at final saved Office 97F4E23C5EBB0779A220CA410204F4713DBCDD9BBD96BF5D29CF8D17D0412BCF and Candidate Cases 49E8A219E79A9E8F72851065CE3CC31CD768E0A38A79093A2711BFA38931D8AA.** Root identified the intervening Office changes as append Loading more copy, initial/empty Loading copy and a known-definite-refusal Refresh button; those exact branches were independently reread. The styling class on the native Adopt control was also observed. They add no financial dispatch, replay, receipt interpretation or scope. Refresh performs load(false) under visible and retains original unknown/accepted journals; it is shown only after the typed definite-refusal notice. Loading remains distinct from genuine native completion/empty.

The optional controller metadata was reread at1775–1778/1790/1818–1828/1847. Only a non-null closed storedCreditScope supplies the stable actor/agency/Candidate qualifier and conditional stored_credit_scope/stored_credit fields. With storedScope null, ordinary version7 keys and snapshot shape remain the original ones. BIND acceptance stores the complete validated native five-field receipt plus the original request_bytes, with original case_id and Candidate pointer. Reopen requires canonical original BIND bytes, exact receipt/command/case identity, no coexisting pending request and exact persisted current scope/epoch. Pending unknown bytes remain the original pending metadata; full account, live bank facts, guessed confirmation or a financial amount are not added to request/journal metadata.

The explicit true currentFacts acknowledgement and hidden-settlement/reconcile fixes in the preceding receipt remain in these final saved bytes. No further concrete blocker was found. This final pin closes the concurrent-source qualification for the reviewed XD02 additions; it does not certify unrelated Office behavior. No test, browser/A4, database, SQL/RPC, business call, network, product edit or release execution was performed. Root's joined A4/runtime and required checks remain independent evidence.

## A4 presentation-support source closure — 13 October 2026

**SOURCE PASS remains for the independently inspected presentation/geometry delta. XC02-01/02 remain closed.** No browser/render/A4 capture or execution was performed by this reviewer.

Office1149–1151 captures the actual active parent stack frame and modal DOM rectangle before opening confirmation. onDismiss1165–1172 restores the rounded existing left/top anchor only while the original actor/owner/parent own guard remains true, the exact captured parentFrame is again stack top, and its kind is native bpay-next-read. It updates only the existing per-kind anchor and compatibility mirror; no adapter or main-file geometry system, unrelated focus or parent reopen is introduced.

Unchanged main337633–337638/337668–337669 confirms the required sequence: sanitize current child geometry, pop child, cleanup invokes onDismiss, then resumeParentAfterChildReturn. Anchor restoration therefore precedes native parent geometry application. Its shape matches get/setSavedModalAnchor328174–328207. Missing rectangle/store or changed frame/actor/owner performs no restoration. The actual own guard includes the owned stored-list frame lifetime and original caller, so another frame of the same kind is insufficient.

The native colgroup, ordinary label/arrow spans, scoped colors and reused question-band/icon alter presentation only. Scoped new CSS adds no !important or command/journal behavior. Known definite refusal uses one sentence, role alert and read-only Refresh; its rendered control checks the same copy value. Unknown and accepted-but-incomplete states retain their original journal/retry/readback meanings.

Success styling applies only to the completed notice after exact Case readback, true affected-parent acknowledgement and finishStoredCredit. Retained pending/accepted metadata suppresses that class. ACCEPTED_PENDING_CASE and disappearance from the original list cannot cause successful materialisation styling. Descriptor, full original request/accepted journal, command routing and native money-owner semantics remain qualified by the preceding receipts.

| Independently inspected checkpoint | SHA-256 |
| --- | --- |
| FE js/bpay-next-office.js | 204BEB679870338BBFA9B40C0F0C73B8230317BD8FE59597497A05139C51C28C |
| FE css/bpay-next-office.css | C8D89B1F7ACE65979697D5B7313E5A5E181802D556E8BFAEB3645A6CA94F5B1E |
| FE js/bpay-next-case-list.js | 49E8A219E79A9E8F72851065CE3CC31CD768E0A38A79093A2711BFA38931D8AA |
| FE js/bpay-next-ui.js, retained adapter | F535E7E75579428CA7B97B57D406CCAEADAEBBF0FE15E5935574F435B772248D |
| FE js/main.js, retained native shell | 9B35F150F430F5AAF9A0FD1741961A86869122FD415F78CDFEEDDF42A09E9856 |

The append guard observed a concurrent Office save to 51E8C91440072DAEB0891892FFDD08FF89F37FBFB11900DFE4CCDD30D052C532. Its complete geometry seam, colgroup and success predicate were immediately reread and remain identical; CSS hash remains unchanged. This receipt certifies those reviewed additions and does not silently certify unrelated concurrent edits elsewhere in Office.

No new concrete source blocker was found. Root owns final A4 captures, checks and release/completion evidence. Reviewer performed no product edit, SQL/RPC/database/business call, test, browser/render, network, Git mutation or deployment.

## Final presentation source pin

**SOURCE PASS at final saved Office SHA-256 51E8C91440072DAEB0891892FFDD08FF89F37FBFB11900DFE4CCDD30D052C532. All reported source findings remain closed.** Root identified the last intervening edit as changing Created cell fact(date(...)) to ordinary el('span',date(...)); the actual saved cell1214 was reread and confirms that presentation change. Native date input/formatting and whole-list Created sort semantics are preserved; amount, command, journal and lifetime logic are unchanged by this cell delta.

This final pin closes the concurrent whole-file qualification above for the reviewed XD01/XD02 changes. CSS C8D89B1F7ACE65979697D5B7313E5A5E181802D556E8BFAEB3645A6CA94F5B1E, Candidate Cases49E8A219E79A9E8F72851065CE3CC31CD768E0A38A79093A2711BFA38931D8AA and retained adapter/main hashes above remain the inspected source inputs. Historical findings/receipts are preserved. No new concrete blocker was found, and no reviewer A4, runtime, test, SQL/RPC/database/business, network or release execution is claimed.
