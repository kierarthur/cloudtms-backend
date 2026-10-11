# EX-01 finite scope and accepted replay investigation

11 October 2026; source-only design support. Parent alone writes product. No product/checklist edit, SQL/database execution, test, fixture, hosted action or installation. Runtime/app acceptance remains **NOT_RUN**. Pending DY-10 closure was not changed.

The full EX-01 card and all fourteen rows were read in `EX-01-current-investigation.md`. This supplement answers the six current protected branches of final `public.bpay_next_financial_intake_v1(uuid,text,jsonb)` in `supabase/repeatable/10102026_2150_banking_pay_next_workbench_retry_v1.sql:200–478` (registry `docs/bpay-next/final-definer-closures.json:7460–7475`). Future protected actions register their contracts here and consume this same proof when their own money owners exist; this investigation does not build them.

## 1. Minimum design conclusion

Use one finite private action-scope resolver, proposed name `private.bpay_next_reauth_scope_v1(uuid,text,jsonb)`, and only the thin service exposure genuinely required by the two named auth routes. It reads the exact native targets below, returns server action/Candidate/GBP amount facts plus the card-owned SHA-256 scope, and performs no financial write. Browser inputs are the current closed action identity/intent, never trusted money totals or scope hashes. Do not add another scope table, external command, queue kind, generic protocol, signing secret or finance calculator.

There are two fundamentally different intake branches:

1. **Unaccepted identity:** authenticate current actor/flags, acquire the native action locks in their existing order, derive the current finite scope, compare it with the verified token/proof, consume proof and call the unchanged native acceptor in the **same transaction**. A plain resolver SELECT before the acceptor obtains its locks is insufficient: the money/control version can change in that gap.
2. **Accepted identity:** qualify the exact native original request/instruction and all input operands, then require the retained proof to have the same actor/action/scope/token hash and consumed native identity. Return through the existing native replay path. Do **not** recompute the scope from today's mutable balances/status/bank details, check first-use expiry, or consume an unused proof for a different action. An expired token may be used only after signature validation plus this exact consumed-proof/original-receipt qualification; it cannot authorize a new identity. Current role/authoriser gates remain enforced.

No additional stored scope facts are required merely to replay: the proof retains the original scope hash, while each native request retains its submitted operands. Once those inputs match, the native original receipt supplies the identity and outcome. Hashing live balance again is the wrong replay test. If native operands are absent, do not invent or reconstruct them from current history; see the narrow gaps below.

Scope should bind authenticated actor, action, native command/instruction UUID, closed typed intent, server-qualified Candidate/beneficiary identity, money basis and relevant version operands. Exclude mutable UI display names and incidental job cursors/status from the hash unless they actually define the permitted action. A first-use state refusal remains the native owner's responsibility; an earlier accepted operation is not made ineligible merely by its successful status transition.

## 2. Per-action native facts and exact replay checks

### WRITE_OFF

**Actual parent:** `private.bpay_next_accept_write_off_v1(uuid,uuid,uuid,bigint,text,text,text)`, final `04102026_1647_banking_pay_next_work_collection_write_off_v1.sql:24–90`.

**Finite current basis:** component PK -> finance Case PK with same Candidate -> work_collection by matching Case/component/Candidate. Native qualification is current final `private.bpay_next_work_collection_origin_v1(uuid,uuid,uuid)`, `10102026_1236_banking_pay_next_segment_authority_final_v1.sql:4461–4514`. Reuse its exact native qualification, not an arbitrary finance Case balance. It checks collection READY/paid basis, current Position/revision and original approved-line evidence, ordinary/Source identity, rule and matching Case/component balances. It can call bounded native segment attribution; it does not select a historical Timesheet by guesswork.

Bind component ID, Candidate, Case/collection ID, expected component revision, current Case revision, principal_approved, principal_recovered, principal_written_off, active_recovery_hold_amount, requested_scope, raw input_amount and exact reason. The native acceptor captures those exact basis scalars (`1647:80–85`). Server preview uses the same native remaining liability `principal_approved - principal_recovered - principal_written_off`: ALL intends that **whole** remaining amount; AMOUNT uses the server-validated existing typed amount. ALL is **not** the unprotected remainder: if protected capacity prevents the whole amount, the original apply owner blocks it. Do not silently clamp. Existing apply derives/checks the amount at `:307–323` and retains REVIEW on changed Case/component revision (`:314–317`).

**Original replay receipt:** write_off_request PK(command_id) + command(kind CASE_WRITE_OFF,current module epoch); request stores actor, component, revision, scope, raw input_amount, exact reason and original basis. The parent already compares the full submitted tuple (`:53–61`) and its receipt returns retained result fields (`:8–20`). Missing native replay operand checks: **none** for its current closed args. Preserve this correct branch.

**Locks:** existing command UUID advisory mutex -> Candidate control -> Case -> component (`:51–72`). The EX-01 admit wrapper must not take these in an inverse order. Preview must not execute a writer or invent another W formula.

### CANCEL_REQUEST

**Actual parents:** simple accept `04102026_0260_banking_pay_next_simple_cancel_v1.sql:95–177`; Case selection dispatches to final `11102026_0559_banking_pay_next_worker_reprepare_v1.sql:3836–3877`. Both retain `private.bpay_next_cancel_request` PK(command_id), command kind SIMPLE_CANCEL. Case scope also retains `private.bpay_next_case_cancel_binding` PK(command_id), draft state and captured preparation/selection revision (`0559:3851–3875`; schema `04102026_0450_banking_pay_next_case_cancel_binding.sql:5–47`).

**Finite current basis:** worker PK -> pay_run PK; Candidate and frozen target channel/umbrella, captured preparation revision, Case selection revision, captured line count, gross_ex_vat/gross_vat/gross_inc_vat, net_request_revision/net_projection_revision. Existing native current NET amount is one `bpay_next_net_projection` row by `(run_worker_id,projection_no=worker.net_projection_revision)` and active retirement predicate; its immutable id/cash_amount/accepted_recoveries describe the accepted cash projection. The existing status reader uses those exact point facts (`10102026_0639_banking_pay_next_ps_c15_reservation_status_v1.sql:409–426`). Whole Candidate cancellation may have a destination group: use the existing complete group/projection scalar totals rather than summing every transfer leg (`0559:3793–3812`).

Do not display NULL cash as zero or label frozen gross as bank cash. Native cancellation intentionally accepts an earlier pending NET/build (`0260:157–161`; `0559:3791,3802–3812`). Where no final cash projection exists, an honest preview can identify the **frozen Draft gross being removed** separately; it cannot claim an exact future bank total. If the intended UI requires bank cash specifically, readiness must be qualified/refused without guessing a new amount or scanning every leg. This semantic choice must be explicit in the build design.

**Replay equality:** simple prior request checks worker/Candidate (`0260:135–148`); Case prior also checks the exact native binding and current command epoch (`0559:3851–3858`). Optional `run_id` + `pay_channel` are independently paired and checked against the same worker (`2150:255–257,395–399`); compare them to the native frozen address on replay too. They are not stored as independent raw request bytes. No reason or amount operand exists in this action; do not add the future scheduled Cancel-selection reason here.

**Gap:** cancel_request has **no actor_user_id** (`03102026_1324_banking_pay_next_core.sql:910–925`). For commands accepted with EX-01, the consumed proof is the original actor evidence; do not infer actor from worker creation. A pre-EX-01 accepted cancellation has no such evidence. That historical receipt can only be treated as an existing permitted read/replay, not fabricated as a newly proof-bound action; no data-history backfill is justified by this card.

**Locks:** Case branch command mutex -> run -> Candidate control -> worker (`0559:3846–3850`); ordinary branch run -> Candidate control -> worker (`0260:127–134`). Preserve native routing and lock order.

### CASH_REISSUE

**Actual accept parent:** final `10102026_0316_banking_pay_next_csv_umbrella_row_projection_v1.sql:341–408`. Native build remains `04102026_0240_banking_pay_next_simple_cash_reissue_v1.sql:71–139`.

**Finite current basis:** return_cash PK -> original_transfer PK -> run_worker/run PK. Bind cash ID, original transfer, Candidate, worker/run, original beneficiary_kind/beneficiary_id and currency, and the server's native available cash scalars amount_owed/amount_held/amount_reissued_paid. Exact whole availability is the existing owner's subtraction (`0316:394–398`; build `0240:118–120`), not additional earnings or another payroll amount. Native admission requires original RETURNED and exact posting-complete return (`0316:381–388`) and no pending request for that cash ID (`:394–398`; partial unique index core:904–905).

**Original replay receipt:** reissue_request PK(command_id), retained return_cash_id/worker/Candidate/actor and optional transfer_id, command kind CASH_REISSUE. If built, join that retained transfer ID and check build_command_id/return_cash_id/original_transfer_id/native worker equality. Native accept replay currently compares **only return_cash_id** (`0316:374–379`). The EX-01 intake qualification must also compare retained actor and exact command kind/current epoch before permitting expired-proof replay; never substitute a different UUID merely because it refers to the same cash.

**Important source limit:** accepted reissue_request has no captured amount or accepted balance revision (`core:888–903`); the existing ordered builder derives whole available cash later under native locks (`0240:98–120`). Therefore replay before build does not have an immutable accepted amount field to re-hash. Use consumed proof + matching request inputs, not current available cash. This also means an assertion that the authorization preview amount is frozen for later build needs source proof of the current whole-cash/pending-writer invariant; if such proof is unavailable, qualify/refuse a genuinely changing pending scope before acceptance. Do not claim that the request stores an amount, add a new scope table, or silently alter the builder formula.

**Locks:** original accept run -> worker -> actor -> cash (`0316:362–394`), builder run -> Candidate control -> job -> request -> worker -> cash (`0240:95–104`). The proof wrapper may re-use the accept locks; it does not adopt the builder's lease or advance the job.

### REISSUE_CANCEL

**Actual parent:** `04102026_0355_banking_pay_next_reissue_cancel_v1.sql:30–75`. Exact transfer PK -> run/worker/Candidate; return_cash and original transfer are explicit native identities. Bind transfer cash_amount, currency, beneficiary identity, original_transfer_id, return_cash_id, member_count/member_cash_sum and exact native reissue member facts. The native block requires one CASH_REISSUE member with the same signed cash contribution and unissued state (`:6–25`); no member-history summation is needed.

**Original receipt:** reissue_cancel_request PK(command_id), command REISSUE_CANCEL/current epoch; request stores transfer, cash, worker/Candidate, actor and exact amount (`:71–72`; migration `04102026_0350_banking_pay_next_reissue_cancel_request.sql:4–24`). Current accept replay compares **only transfer_id** (`:55–62`). Add retained actor/native identity/kind/epoch equality in the EX-01 intake resolver; amount comes from the captured request on replay, not current cash.amount_held. The apply owner already checks the full frozen transfer/cash/request amount tuple (`:145–150`) before releasing the exact reservation (`:154–159`).

**Locks:** run -> Candidate control -> worker -> transfer -> actor (`:43–53`); original apply subsequently locks cash. Leave that money owner intact.

### CSV_ISSUE

**Actual parent:** final `10102026_0316_banking_pay_next_csv_umbrella_row_projection_v1.sql:138–333`. Intake `2150:428–436` already takes the parent run mutex before instruction lookup and binds the approved destination only for first issue.

**Finite current basis:** transfer PK -> worker/run/Candidate; immutable cash_amount/currency/projection/native beneficiary ID plus confirmed Draft and current complete member totals. Bank scope must use the **actual to-be-bound native destination**, not the transfer's currently NULL/unbound snapshots. Candidate and Umbrella paths are the exact branches in binder `0316:76–99`; ONEOFF follows destination leg -> net destination amount -> retained stored_credit_origin (`:101–106`). A pure preview should point-join those exact facts and current native bank qualification; do not call a locking/write helper merely to obtain preview. The final execution-leg helper currently may lock the one-off bank row (`0639:49–63`), so it is not a drop-in lock-free preview dependency.

Bind actual beneficiary kind/id, native bank_details_hash (and source authority/version for one-off), cash amount, transfer ID and **instruction UUID**. Keep existing issue-time current bank checks (`0316:215–286`) and settings format/column qualification (`:288–301`); scope rechecking must not remove them. Native transfer money and identity are immutable through status transitions (`10102026_0533_banking_pay_next_ps_c01_simple_transfer_bridge_v1.sql:243–299`).

**Original receipt:** csv_instruction PK(id), unique transfer_id, actor_user_id, frozen bank hash, cash, payment reference, exact file bytes/hash/time (`core:761–776`; `0316:319–323`). Native replay checks only instruction→transfer (`0316:195–207`); EX-01 consumed-proof replay adds original issuer equality and exact proof's native instruction UUID. No bpay_next_command row exists for this issue: **do not add a consumed_command_id FK requiring one**. Same original instruction retrieves the exact stored bytes after bank/status changes; a different instruction UUID for an already-issued transfer still refuses (`:209–212`). First issue and permitted redownload must remain distinct; no new issue is authorized by retrieval.

**Locks:** run -> worker -> transfer -> actor, then current Candidate/Umbrella bank facts (`:176–194,238–260`). Admit must derive/compare the actual destination while those native conflicting locks are held, before the existing destination mutation/issue.

### CSV_RETURN

**Actual parent:** `04102026_0230_banking_pay_next_simple_csv_return_v1.sql:5–81`. Target transfer PK -> worker/run/Candidate. Native amount is immutable transfer.cash_amount; exact predecessor settlement request/outcome establishes SETTLED whole-transfer amount and occurred_at<=return timestamp (`:58–64`). Existing amount/receipt/timestamp validators remain (`2150:312–330`). Scope binds transfer, Candidate/worker/run, GBP whole-transfer amount, receipt_id and typed occurred_at, actor and original command UUID. No inferred partial return or reconstructed cash value.

**Original receipt:** return_request PK(command_id) -> unique outcome_id -> transfer_outcome PK/unique receipt_id. Compare request command_id/actor/transfer/worker/Candidate and command CSV_RETURN/current epoch; compare outcome kind RETURNED, transfer_id, exact receipt_id, whole_transfer_amount and occurred_at_utc. The native parent compares the outcome tuple (`0230:44–48`) but then finds return_request by outcome and returns **its** command ID (`:50–52`), ignoring submitted p_command_id and actor. It therefore permits another UUID to reach receipt-based deduplication. **Required narrow EX-01 correction:** intake/resolver must refuse changed command UUID or actor before consumed-proof replay, while preserving the native receipt's status/posting behavior and original equality of amount/receipt/timestamp. Do not allow proof binding to a new UUID whose native call merely returns someone else's older receipt.

**Locks:** run -> Candidate control -> worker -> transfer -> actor (`0230:32–43`). No new Work/Case locks or return-money producer is required.

## 3. Strict operand and proof qualification

The current intake already validates exact closed key sets, UUID strings, finite decimal money, receipt byte bound and finite timestamp (`2150:248–332`). Keep that qualification before the resolver. Re-auth payload itself must not make an unprotected action accepted or change those bounds.

- WRITE_OFF: compare raw reason and input_amount exactly as the native parent already does; do not trim, normalize or replace them in replay.
- CSV_RETURN: compare semantic numeric amount and timestamptz to the native stored outcome, with exact receipt string. Native data stores typed time, not the original timezone spelling; two strings denoting the same instant are the same stored financial operand. The browser's original journal still retries identical serialized bytes. Exact byte equality of an old timestamp spelling cannot be reconstructed from this schema and must not be falsely claimed.
- Other four actions: no amount/reason/receipt/timestamp operand exists; native target identity is the submitted financial scope. Optional cancel address must resolve to the same immutable worker/run/channel. UUID casing is semantic identity; first-use scope canonicalization must be fixed and shared across START/intake.
- After exact native equality, compare the verified token SHA-256, actor, action, stored scope hash and consumed native UUID. A body change cannot be accepted just because the supplied JWT is valid. The record's unique token hash returns the same proof for an identical verified record retry; it cannot authorize a second consumed UUID. Do not update stored scope_hash on replay.

An exact consumed proof is a receipt qualification, not permission to bypass current admin/payment flags. Native public intake and relevant callees already require those gates. Accepted expiry replay needs the same valid signature and stored proof identity, with expiry ignored **only after** exact original native receipt equality. First use retains expiry and fresh current scope qualification. Recorded-but-unconsumed proof is not an accepted financial command; unknown record reply recovers the same proof but does not bypass first-use expiry.

## 4. Narrow supporting code and remaining design choices

Required support can stay in the exact current2150 intake successor plus the new card-owned proof/resolver repeatable and Worker/auth/Office seams. Add the missing replay identity/actor checks above before calling unchanged native owners; no wholesale rewriting of cancel/reissue/return/job/page bodies is required merely for proof replay. Do not tighten correct WRITE_OFF replay again or use live balances to invalidate successful old receipts.

The current facts leave two design choices that must be stated honestly: CANCEL_REQUEST preview wording when an earlier NET/build is pending (frozen Draft gross versus currently accepted cash), and proof of whole CASH_REISSUE availability stability from acceptance to the original deferred build. They are not grounds for a new scope storage/protocol or early future money implementation. First-use race closure uses existing native transaction locks; preview uses bounded point reads. No source or deployment PASS beyond this investigation is claimed.

## 5. Follow-up: whole CASH_REISSUE availability is stable on the current native lifecycle

**Conclusion:** the current native source proves that a genuine whole returned-cash pool with available>0 cannot have its availability increased between accepted0316 reissue and original0240 build. No genuine sequence using the current registered writers was found that changes that positive availability. This closes the CASH_REISSUE uncertainty in section4; no additional scope table, column or money-owner rewrite is required for BD110's native whole-pool authorization. Future transfer issuance separately re-authenticates its actual frozen cash. This is a source lifecycle proof, not observed-row or runtime proof.

### Exhaustive saved-source writer census

A search of all current repeatable SQL and Worker source found only these balance writes; historical0200 repeats the final2018 settlement body rather than being another current owner. Registry final owners were checked directly.

| Current native writer | Exact balance effect | Authority/replay fences |
| --- | --- | --- |
| `private.bpay_next_post_simple_return_v1(uuid,uuid,bigint)`, final0230 | Original return inserts O=transfer.cash_amount with H=P=0 defaults (`0230:133–135`; core:700–713). A returned **reissue** decreases P by that whole transfer.cash_amount in the same existing pool (`:137–146`). No UPDATE changes O. | Root lock/Candidate/job/request/worker/transfer `:104–112`; old DONE+posting_complete returns before writes `:117–123`; exact lease, outcome posting and earlier-command order `:125–131`; reissue pool must match original transfer/Candidate and have P>=whole amount `:139–146`; request marked posting_complete and job DONE atomically `:149–152`. |
| `private.bpay_next_build_simple_reissue_v1(uuid,uuid,bigint)`, final0240 | Computes A=O-H-P once, creates a transfer/member for **all** A, increases H by A and sets request.transfer_id in the same transaction (`0240:118–134`). | Same root/Candidate/job/request/worker/cash locks `:95–104`; DONE+transfer returns before mutation `:109–110`; exact lease and no earlier unfinished command `:112–116`. |
| `private.bpay_next_post_simple_outcome_page_v1(uuid,uuid,bigint,bigint,integer)`, final2018 | For the one CASH_REISSUE member, decreases H and increases P by the same whole transfer.cash_amount. A is unchanged (`09102026_2018:2387–2408`). | Same root/Candidate/job/request/worker/transfer locks `:2333–2345`; DONE receipt and cursor equality prevent repeat `:2367–2384`; exact pool/original/Candidate/H qualification `:2387–2393`; one member and matching contribution `:2400–2403`; checkpoint and DONE committed with the page `:2510–2528`. |
| `private.bpay_next_apply_reissue_cancel_v1(uuid,uuid,bigint)`, final0355 | Decreases H by retained request.amount, the exact whole built transfer.cash_amount, reopening the pool (`0355:145–159`). | Root/Candidate/job/request/worker `:126–133`; DONE returns before mutation `:137–138`; exact lease `:140–143`; frozen pool/transfer/amount tuple `:147–150`; H>=request.amount and original unissued-transfer block `:152–155`; transfer CANCELLED/request terminal atomically `:157–159`. |

Core fixes positive O, nonnegative H/P and H+P<=O, with one unique pool per original transfer (`03102026_1324_banking_pay_next_core.sql:700–713`). No current writer changes O, deletes a pool or makes a partial reservation/payment. The member origin guard binds one CASH_REISSUE member to the native pool, original transfer, request and exact contribution (`10102026_0554_banking_pay_next_ps_c03_member_origin_guard_v1.sql:55–69`); transfer cash/identity are immutable (`10102026_0533_banking_pay_next_ps_c01_simple_transfer_bridge_v1.sql:243–299`).

### Reachable-state argument and pending-command exclusion

Write O=amount_owed, H=amount_held, P=amount_reissued_paid, A=O-H-P. This notation describes the existing formula; it introduces no new calculator.

1. Native original return creates `(H,P)=(0,0)`, so A=O.
2. The only builder reserves the whole available O, moving to `(O,0)`, A=0. Its pending request receives transfer_id atomically with that reservation.
3. Settlement of that whole transfer moves `(O,0)` to `(0,O)`, keeping A=0. Cancellation moves `(O,0)` back to `(0,0)`; a genuine return of a paid reissue moves `(0,O)` back to `(0,0)`.
4. Thus current native cycles have only `(0,0)`, `(O,0)`, `(0,O)`. At admission A>0 means `(H,P)=(0,0)`. No balance-increasing writer can still apply: cancel requires a matching positive H reservation, and reissue-return requires positive P equal to its whole transfer amount. Both are absent. A settlement cannot increase A.
5. A second builder cannot intervene: current accept rejects any existing unbuilt request for the same cash ID (`0316:394–398`), the partial unique `bpay_next_reissue_pending_cash_once_idx` enforces one request with transfer_id IS NULL (`core:904–905`), and the first builder simultaneously reserves all A and populates that transfer_id (`0240:125–134`). After build the pool has A=0, blocking any new acceptance until a genuine cancellation or return restores it.

All supported pool writers use the same parent run mutex through the original worker association, and current job writers also retain exact Candidate command order. An earlier accepted cancellation/return can remain REQUESTED, but while its necessary H or P is positive the new reissue's A>0 admission is impossible. If the earlier writer already restored A, its terminal receipt/checkpoint prevents a second restoration. A later CSV settlement/return cannot apply before its earlier reissue builder has created the actual whole transfer and the native preceding outcomes; it cannot enlarge A before that builder.

**Boundary:** the core CHECK alone permits arithmetically valid partial H/P values; it does not encode the stronger three-state lifecycle. A manually edited or separately imported partial pool could admit A>0 while an older partial cancellation/return later increases it. That is a counterexample to a claim about *every arbitrary row satisfying the CHECK*, but it is not reachable from the current authorized native writers found above. No such row was queried, assumed or created. The source conclusion is specifically the current card-owned native whole-pool action, as BD110 proposes; it must not be advertised as an invariant of future partial reissue writers or unauthorized data changes.

The resolver can use the exact native pool identity and current whole availability for the pre-auth preview, recheck under the existing run/cash locks at first acceptance, and allow original consumed-proof replay from retained request identity afterward. The unchanged0240 whole-pool builder remains the owner of reservation. No early money rewrite or additional proof storage is needed to make this current source design coherent.


