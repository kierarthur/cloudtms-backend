# PS-14 concrete reservation read design — 10 October 2026

Source-only design; no SQL/runtime/tests/data/app/network execution and no product edits. This refines `reviews/PS-14-scope-investigation.md`. The four original2025 money owners are correct reuse: preserve their bodies and include them through the scoped successor. New work is a private STABLE reader, existing0580 STATUS integration, exact typed Worker/Office fields and D04 future acceptance. No new cash/hold counter or financial producer is justified.

## Recommended finite contract

Use mutually exclusive **original obligation before full materialisation** and **original legs after full materialisation** modes. Keep the existing accepted-projection header informational. It is never added to reservation legs.

| Mode | Header reservation cash | Each emitted original transfer's reservation cash |
| --- | --- | --- |
| UNCONFIRMED | NULL; not a confirmed reservation | NULL |
| PENDING_NET / PENDING_PREPARATION | NULL; known incomplete acceptance, old accepted cash remains separately labelled | No invented current obligation |
| ORIGINAL_OBLIGATION | Exact immutable projection cash; ordinary Umbrella before projection uses its already confirmed frozen gross under the original eligibility clauses below | NULL with INCLUDED_IN_ORIGINAL_OBLIGATION; native transfer cash remains ordinary informational transfer data |
| ORIGINAL_LEGS | NULL; means deliberately paged per-leg facts, **not an unknown total** | Exact server-derived native leg cash or0 with explicit lifecycle state; genuinely unknown provider execution is NULL |
| CANCELLING / CANCELLED | Exact0 after the native whole-worker cancellation fence | Exact0 with cancellation state, even while source/Case hold release or leg cancellation is still progressing |
| COMPLETE | Exact0 where every original leg's actual receipt/posted completion establishes closure | Exact0; retained paid/original facts remain separately available |

This avoids an unbounded whole-worker/group cash aggregate and requires no new request cursor or public API. Existing transfer keyset `(run_worker_id,transfer_no,id)`, requested1..100 and original transfer rows remain the only page. A complete group has no unmaterialised obligations; a partial group cannot execute any leg, so one original-vector cash fact is exact throughout its partial materialisation. Suppress per-leg counted reservation in that mode rather than publishing a false per-leg0. Once all legs are complete, the header explicitly switches to paged per-leg authority. Office shows each server fact once and never totals pages or distributes NET by Client/Timesheet.

**If a whole-Candidate remaining total is chosen instead:** a SQL SUM over the exact original group-leg set is financially valid, unlike member SUM, but is O(original legs) and cannot honestly inherit the100-row processing bound. Do not silently use a truncated SUM or add a stored counter. That alternative needs an explicit processing-budget decision. The paged contract above supplies all valid ordinary/Case-only/split cases without a blanket unsupported state.

Suggested small exact fields (names are builder choices): header `reservation_mode`, `reservation_projection_id` nullable, `remaining_reservation_cash` decimal-text nullable, `reservation_internal_zero` boolean, `reservation_holds_state` finite enum. Transfer row `reservation_state` finite enum and `remaining_reservation_cash` decimal-text nullable. In ORIGINAL_LEGS header NULL is a typed paging mode, not PENDING. Count need not be added: an internal-zero row/mode distinguishes genuine0 from no evidence. Preserve existing STATUS/CHANNEL_STATUS identity, command replies, journal, retry and queue wake.

## 1. Resolve exact native scope once

Read one point scope `r.id=p_run_id AND w.id=p_worker_id AND w.run_id=r.id`; retain actual `w.candidate_id`. Original0580 rejects missing scope and checks module owner. Reuse that authority and existing public2018/F08 channel checks. Do not add business locks, bank detail reads, agency commands, today's finance settings or WORK.current_revision_id.

A confirmed obligation requires `r.confirmed_at_utc IS NOT NULL`. Run cancellation and worker lifecycle are facts, not a requirement that every read remain READY. In particular, `w.status='READY'` remains true after one destination leg settles until all posting finishes; worker status alone is not an unpaid-leg proof.

Locate group by its unique `run_worker_id`; original non-group transfer is `run_worker_id=w.id AND original_transfer_id IS NULL AND return_cash_id IS NULL` (native first transfer_no1, no group). Reissue `original_transfer_id IS NOT NULL` is never a second parent obligation; classify it separately/excluded even when returned-cash reissue rows are on the same STATUS page. Resolve original projection by `g.projection_id` or `t.projection_id` whenever materialised. Do not substitute the current header projection, reject it just because retired, or select newest historical projection.

Native point scopes:

```sql
-- Plain read fragments; design only, not executed.
select g.* from private.bpay_next_destination_group g
where g.run_worker_id = w.id;

select p.* from private.bpay_next_net_projection p
where p.id = coalesce(g.projection_id, original_t.projection_id)
  and p.run_worker_id = w.id;

-- Only with NO original transfer/group: current accepted PAYE projection.
select p.* from private.bpay_next_net_projection p
where p.run_worker_id = w.id
  and p.projection_no = w.net_projection_revision
  and p.retired_at_utc is null;
```

Point/read tuple assertions use `IS DISTINCT FROM` for nullable original IDs and `IS NOT TRUE` for required booleans; missing basis is a finite evidence refusal, never optimistic0.

## 2. Ordinary Umbrella and no-group PAYE

**Umbrella is a necessary distinct branch.** Final2018 begin_simple_transfer1524–1649 creates its projection only on transfer begin. Before that, the legal ordinary worker has `target_pay_channel='UMBRELLA'`, `case_selection_revision=0`, `net_request_revision=0`, `net_projection_revision=0`, no PAYE request, `entered_paye_net IS NULL`, nonnull target Umbrella/VAT capture, `gross_inc_vat>0`, `captured_line_count>0`, `financial_resolution_count=0`, `expected_managed_position_count=0`, status READY and confirmed DRAFT. Exact held frozen WORK scope supplies an ORIGINAL_OBLIGATION of `w.gross_inc_vat`, one Candidate-to-Umbrella cash obligation. This is retained frozen money, not a new projection or live reprice.

Begin creates immutable UMBRELLA projection_no1 and sets only net_projection_revision1; request stays0. Therefore generic STATUS `net_pending=(request<>projection)` is **not** an economic replacement rule for this valid0/1 branch. Materialised ordinary Umbrella resolves exact original transfer/p, verifies original UMBRELLA tuple and uses native transfer cash. Do not change unrelated current STATUS pending semantics to build PS14.

No-group PAYE before transfer uses the actual accepted projection with `net_request_revision=net_projection_revision>0`; no-case simple first projection has PAYE_MANUAL/input-request identity and cash equal entered NET as original2018. Case PAYE requires actual current completed DRAFT/NET state pair below. If a PAYE replacement request is accepted but not yet projected, use PENDING_NET/NULL for current reservation while showing old accepted projection as old accepted data. This is a real partial-replacement state, not refusal of ordinary valid READY workers. Original0390 permits replacement only while **no transfer exists** (75); it can release prior NET recovery holds before retiring/switching projection at final509–531. No browser subtraction of those partial releases can derive the new cash amount.

With a single native transfer: BUILDING is a known original obligation, not a missing-member test; after MEMBERS_READY the per-leg native cash is authoritative. There is exactly one original transfer, so it is safe to show its server-classified remaining cash in the header as well **only if explicitly labelled the same fact and not included twice**. The consistent implementation is to use ORIGINAL_LEGS for single and split alike.

## 3. Case-only and destination-vector basis

For accepted Case PAYE/CASE_PAYOUT, retain exact projection and its `case_allocation_state` NET row:

```sql
s.projection_id = p.id
and s.run_worker_id = w.id
and s.pass_kind = 'NET'
and s.status = 'COMPLETE' and s.net_stage = 'COMPLETE'
and s.projection_no = p.projection_no
-- Match captured preparation/selection tuple, and exact completed DRAFT state.
```

Use captured state/group preparation/selection revisions once materialised. Current worker pointers are not historical substitutes. `captured_line_count=0` is legal Case-only; do not require WORK members or WORK holds. Original0410 begin6–97 already certifies the completed DRAFT/NET pair, immutable projection and original job DONE/PROJECTED.

A destination vector exists only if original1827 `net_destination_state` was actually created. Absence is a genuine single Candidate obligation, not an error: seal_net_destination185 returns the whole projection cash when no destination row exists. If present, require exact `projection_id=p.id`, `run_worker_id=w.id`, completed timestamp, `cash_amount=p.cash_amount`, `own_amount+external_amount=p.cash_amount`, external_leg_count>0/external_amount>0. OWN and external obligations are disjoint; never multiply p.cash by the number of external credit instructions.

Canonical external obligation: `net_destination_amount.state_id=n.state_id`, actual destination_id, native `amount>0`, first_origin_command_id. Actual materialisation link is group_leg.destination_id with exact anchor; OWN is group_leg.kind OWN and NULL destination. Do not disclose bank hash/detail fields. Partial materialisation requires `g.projection_id=p.id`, `g.net_state_id=n.state_id`, exact worker/Candidate tuple, `g.expected_leg_count=n.external_leg_count+1`; existing created/sealed counts and stage are native progress. While `g.stage<>'COMPLETE'`, original2025 refuses **all** leg execution. The remaining confirmed whole original vector is therefore exactly p.cash once, not p.cash plus created leg cash.

For COMPLETE group require completed timestamp, created_leg_count=sealed_leg_count=expected_leg_count, sealed_cash_total=p.cash_amount, n identity/conservation. These are already guarded stored facts. Per emitted original transfer resolve its group_leg, require exact anchor and p id. OWN amount=n.own_amount; ONEOFF amount=actual net_destination_amount.amount. Verify transfer.cash_amount equals that native obligation. No need to scan all members or all destinations per emitted row.

## 4. Active holds — exact scope, not a NET amount allocator

Use real hold status to distinguish reserved source/Case capacity from paid/released capacity, but never SUM gross/source hold amounts into a share of Candidate bank NET. Actual indexed evidence fragments:

```sql
-- An actual captured WORK hold, including before member materialisation.
exists (
 select 1 from private.bpay_next_run_line l
 join private.bpay_next_hold h on h.run_line_id=l.id
 where l.run_worker_id=w.id and h.status='ACTIVE'
   and (h.work_id,h.component_key,h.source_reserved_ex_vat,
        h.target_amount_ex_vat,h.target_amount_vat,h.target_amount_inc_vat)
       is not distinct from
       (l.work_id,l.component_key,l.source_consumed_ex_vat,
        l.frozen_ex_vat,l.frozen_vat,l.frozen_inc_vat)
)

-- Case reservation from the original accepted result/state, not all retired NETs.
exists (
 select 1 from private.bpay_next_case_hold h
 join private.bpay_next_case_allocation_result ar on ar.id=h.allocation_result_id
 join private.bpay_next_run_case_instruction i on i.id=h.instruction_id
 where h.run_worker_id=w.id and h.candidate_id=w.candidate_id
   and h.status='ACTIVE'
   and ar.state_id in (original_draft_state_id, original_net_state_id)
   and ar.instruction_id=i.id and i.run_worker_id=w.id
   -- actual purpose/source/target tuple is the existing typed FK/shape authority
)

-- After materialisation restrict positive hold evidence to THIS original leg.
exists (
 select 1 from private.bpay_next_transfer_member m
 join private.bpay_next_hold h on h.run_line_id=m.run_line_id
 where m.transfer_id=t.id and m.subject_kind='WORK' and h.status='ACTIVE'
)
or exists (
 select 1 from private.bpay_next_transfer_member m
 join private.bpay_next_case_hold h on h.id=m.case_hold_id
 where m.transfer_id=t.id and m.subject_kind='CASE' and h.status='ACTIVE'
   and h.run_worker_id=w.id and h.candidate_id=w.candidate_id
)
```

Do not impose 'every original row has a hold'. Originalfresh holder1382 explicitly creates certified-zero lines without holds; originalCase member183 forbids a hold for a zero allocated result. An INTERNAL_ZERO obligation can have zero cash yet genuine positive held WORK/Case source to post, or all genuine certified-zero constituents. Preserve an explicit zero/internal obligation state. A positive outstanding native leg with neither active original WORK/Case capacity nor a genuine typed positive reservation basis is an evidence conflict; do not label it unexecuted solely from absent outcome. Existing insertion/transition guards establish full materialisation scope; these EXISTS probes report retained active state rather than rebuild all member conservation on every read.

Cash lifecycle predicates below must win **before** active-hold presence: settled intake preserves active holds while asynchronous posting continues; cancellation preserves active holds while paged release continues. Thus holds do not reinstate paid or cancelled original cash.

## 5. Per-original-leg precedence and exact receipts

1. Reject original identity/amount conflict. Reissue is `EXCLUDED_RETURN_CASH_REISSUE`, not a parent original leg.
2. Exact native CANCELLED or whole-worker cancellation fence =>0, cancellation label; never RETURNED.
3. Actual original SETTLED receipt =>0, label EXECUTED_PENDING_POSTING or POSTED. Match `outcome.transfer_id=t.id`, `outcome_kind='SETTLED'`, `whole_transfer_amount=t.cash_amount`, and actual outcome_request.outcome_id/outcome.transfer/run_worker/Candidate tuple. Final2018 settlement2277–2285 inserts receipt+request+SETTLED atomically before posting. Native destination_posted_leg.original_transfer_id=t.id confirms posting, not an extra amount.
4. Actual RETURNED receipt =>0 for original Draft reservation; retain the paid-origin and separate return_cash original_transfer_id lifecycle. Neither returned cash nor cash-only reissue is an unexecuted original Draft reservation.
5. INTERNAL_ZERO: cash must0. Original internal_receipt.transfer_id unique, actual worker/Candidate/projection tuple and matching outcome_request.internal_receipt_id establish accepted internal execution. INTERNAL_PROCESSING =>0 bank cash, INTERNAL_PENDING_POSTING; INTERNAL_SETTLED =>0 CLOSED_INTERNAL. Before receipt =>0 cash with INTERNAL_RESERVED obligation label. It is not bank payment, no null/zero conflation, and its positive held source can remain pending. Do not complete the whole worker just because OWN internal-zero completed while external legs remain.
6. SUBMITTED/UNKNOWN without a closed actual outcome =>NULL, EXECUTION_UNCERTAIN. Native pending external execution does not establish unexecuted cash. Preserve t.id/p.id and held capacity state. No provider support added.
7. Other actually supported unexecuted original states BUILDING/MEMBERS_READY/DRAFT/SCHEDULED/REFUSED =>exact t.cash_amount, with active original reservation evidence and no execution/cancel receipt. BUILDING belongs to header ORIGINAL_OBLIGATION mode until full group materialisation, so row state is INCLUDED_IN_ORIGINAL_OBLIGATION with NULL counted amount.
8. ISSUED_CSV without actual accepted settlement =>exact t.cash_amount **as outstanding held reservation awaiting recorded execution**, state ISSUED_AWAITING_RECEIPT; never label it physically proved unpaid in the bank. Existing immutable CSV instruction is actual issue evidence; issue is not an accepted SETTLED outcome. Once intake accepts settlement, step3 wins even if holds still ACTIVE. This is an ordinary supported CSV state, not blanket unavailable.

Status/receipt contradictions (SETTLED without its actual immutable receipt, INTERNAL_SETTLED without receipt, CANCELLED with actual SETTLED money, wrong amount/scope) require a typed evidence refusal or explicit conflict state, never infer0 solely from a terminal-looking word. Query actual outcome kinds independently; do not choose a 'latest' guessed receipt. Existing native receipts/request keys provide exact scope; both SETTLED and later RETURNED may legitimately exist as different immutable lifecycle facts.

## 6. Native cancellation proof and partial cancellation

Single simple0260 first revalidates block/DRAFT/confirmed/expected lines, then atomically sets worker CANCELLING, original transfer CANCELLED and request CANCELLING (335–369), before hold release. Case non-group0460 similarly binds accepted exact p/state and sets its native original transfer CANCELLED (236–255). Therefore remaining bank reservation is0 while releases progress; active held source/capacity is reported as releasing, not spendable cash or returned cash.

Group2102 **first checks every original leg** in <=100 pages while request remains REQUESTED; checked_leg_count=expected_leg_count, each actual leg MEMBERS_READY, no approval, CSV instruction, outcome or internal receipt. While only REQUESTED/checking, reservation remains unchanged. Once full check passes, original0460 captures p/revision, sets binding CANCELLING and worker CANCELLING before WORK/CASE releases. Native execution cannot enter after that fence. FINAL then cancels original legs in <=100 pages. Thus partially marked CANCELLED legs are not evidence that other legs remain executable; the native whole-worker fence makes all original obligations non-executable already.

Exact group cancellation proof:

```sql
exists (
 select 1 from private.bpay_next_case_cancel_binding b
 join private.bpay_next_cancel_request cr on cr.command_id=b.command_id
 join private.bpay_next_destination_cancel dc on dc.command_id=b.command_id
 where b.run_worker_id=w.id and b.candidate_id=w.candidate_id
   and b.transfer_id=g.anchor_transfer_id
   and b.projection_id=g.projection_id and b.net_state_id=g.net_state_id
   and b.preparation_revision=g.preparation_revision
   and b.selection_revision=g.selection_revision
   and b.accepted_projection_revision=p.projection_no
   and b.status in ('CANCELLING','CANCELLED')
   and cr.status=b.status
   and dc.anchor_transfer_id=g.anchor_transfer_id
   and dc.expected_leg_count=g.expected_leg_count
   and dc.checked_leg_count=dc.expected_leg_count
   and w.status in ('CANCELLING','CANCELLED')
)
```

Tie any used SIMPLE_CANCEL command to this exact request's command_id/native expected/enrolled single-Candidate command member scope. Do not require command COMPLETE or all leg CANCELLED while a valid cancellation is still progressing; that would miss the real early fence. Retained binding CASE/WORK release counts are audit/progress, never numeric bank subtraction. A contradictory worker CANCELLING without its native request/binding is conflict, not automatically0. Blocked cancellation leaves reservation intact. No new cancellation behavior or reclaim/reissue producer is needed.

## 7. Paging, snapshot, timeout and exact indices

Keep STABLE write-free helper + STABLE worker_status_page: one SQL invocation supplies a consistent read snapshot for header, cancellation/receipt facts and emitted rows. No row/advisory locks; do not call destination_execution_leg from the reader because it takes live bank FOR SHARE and refuses incomplete groups. Different keyset pages are live reads, not a cross-request frozen snapshot. Include mode/projection each page; if materialisation mode changes, replace the display's current reservation presentation rather than concatenate cash from old and new modes.

Existing transfer page maximum100, SQL header/rows ceiling120000 bytes, Worker request32768 bytes/response128KiB/8s deadline are retained. The header must be included in the byte budget. Existing complete logic uses full page as nonterminal and one final empty page if needed; never claim whole-leg completion from a truncated byte page. Existing public wrapper/privacy/service-only authorization stays unchanged.

Scope joins use these actual indices/uniques:

| Relation/read | Existing exact index/key |
| --- | --- |
| run/worker | pay_run PK; run_worker PK; unique(run_id,candidate_id) |
| projection | PK(id); unique(run_worker_id,projection_no) |
| original transfer page | bpay_next_transfer_worker_idx(run_worker_id,transfer_no,id); unique(run_worker_id,transfer_no) |
| exact group | unique(run_worker_id), PK(anchor_transfer_id), unique(projection_id) |
| NET destination state | unique(projection_id), PK(state_id) |
| external obligation / exact leg | destination_amount PK/ bpay_next_net_destination_page_idx(state_id,destination_id); group_leg unique(destination_id), PK(transfer_id), bpay_next_destination_group_own_idx(anchor) WHERE OWN |
| exact posted original leg | posted_leg PK(original_transfer_id) |
| exact outcome | bpay_next_transfer_outcome_idx(transfer_id,occurred_at_utc,id); immutable outcome/request identity keys |
| internal zero | internal_receipt unique(transfer_id), outcome_request unique internal_receipt_id |
| cancel fence | bpay_next_case_cancel_worker_idx(run_worker_id,command_id); destination_cancel PK(command_id), bpay_next_destination_cancel_anchor_idx(anchor_transfer_id,command_id); request worker index |
| captured WORK/holds | bpay_next_run_line_page_idx(run_worker_id,line_no,id); hold unique(run_line_id) |
| active Case scope | bpay_next_case_hold_active_worker_page_idx(run_worker_id,id) WHERE ACTIVE; hold unique(allocation_result_id); active_instruction unique(instruction_id) WHERE ACTIVE |
| member/hold evidence | bpay_next_transfer_member_page_idx(transfer_id,member_no), hold PK/unique(run_line_id); result/instruction PK |

No new indices are essential for point original-leg facts. EXISTS over a leg's members or worker lines is potentially proportional to that actual leg/worker when proving absence; early positive presence normally stops quickly. Do not falsely call that a strict100-row CPU bound. For a strict finite visited-row contract, reuse native guarded materialisation/cancel/receipt authority for classification and return separately paged active-hold details rather than audit every member on the STATUS hot path. A100-transfer page is not proof each transfer has <=100 members.

The8s Worker abort is a transport bound, not proof that an already-running top-level PG statement is8s. Preserve actual existing request-budget wrapper/top-level statement controls (PF04) and do not invent a function SET statement_timeout success claim. Source-only design has no measured timing receipt. Genuine timeout returns typed retryable refusal, not partial optimistic money.

## Bounded open decisions and implementation closure

1. **Recommended** paged native-leg authority avoids a whole-group remaining total. If root chooses whole total, explicitly accept proportional current-leg processing under the existing actual SQL budget; no truncated total/new counter.
2. Active-hold reporting must not turn an all-member integrity audit into every STATUS request. Existing guarded native status/materialisation and immutable outcomes/cancel fence already certify original authority; active hold presence/capacity is supporting current evidence, not NET allocation.
3. Final exact field names/enums are builder choices; decoder NULL must distinguish ORIGINAL_LEGS (paged, known) from PENDING_NET/EXECUTION_UNCERTAIN. Ordinary Umbrella0/0 then0/1, completed no-destination Case, Case-only0WORK, internal0 and partially cancelled group are mandatory positive/finite branches, not unsupported defaults.
4. Existing four2025 definitions, original0390/0410/0460/2102/2018/0533/0490 financial behavior stays reused. No Case rewrite/chain jump/Rule-B alteration or new Summary layout.

D04 NOT_RUN clauses: before build ordinary Umbrella and PAYE; partial split build all-cash counted once; complete own/external legs; one settled receipt while its posting/holds remain pending; opposite external/own execution order; Case-only0WORK with positive cash; real internal0; REQUESTED group check versus actual CANCELLING/partialFINAL; no-group replacement midway hold release; cancelled audit is not returned cash; returned original/reissue separate; GBP300 across two Clients one original amount with PS03 constituent IDs; foreign scope/conflict/timeout refusal and live paging mode change. No app/business acceptance has been executed here.

## Current-source evidence anchors

- Card06:1616–1634; current checklist846–870, D01/D04/D05/D06.
- Original2018 simple begin1524–1649; actual Umbrella projection creation1593–1602; settlement2191–2290; original posting2298 onward.
- Original0390 case NET accept64–95 and no-transfer75; replacement final509–531.
- Original0410 case begin38–92; actual zero hold exemption183 and exact member-hold185–190.
- Original1827 net destination seal178–194; group build359–535.
- Original2025 execution group closure30–46 and bank FOR SHARE55–56; posted/internal receipt guard116–123.
- Original0260 simple cancel335–369, retire/cancel451–467; original0460 case/group fence207–265 and FINAL350–365; original2102 full CHECK83–129 and FINAL133–168.
- Original0490 internal receipt141–145 and internal completion193–241.
- Original0580 worker STATUS59–123; Worker timeout constant6 and handler388–405.

## Fresh raw source SHA256

| BUILD-BE relative path | SHA256 |
| --- | --- |
| `supabase/repeatable/04102026_2025_banking_pay_next_destination_execution_v1.sql` | `5ed7e53c884b507444bda67c7ce6061a82c6b8690a3461ffa80846aaa98c5d1b` |
| `supabase/repeatable/04102026_0580_banking_pay_next_office_status_v1.sql` | `ab95db05d07150d131ba027951878b9833e4e5b5e1cb9f4873cfa55fddc3a1ba` |
| `supabase/repeatable/04102026_0390_banking_pay_next_case_paye_net_v1.sql` | `9622c5b9259f80e69a4fb661425c0016a4f3ee69ee8b02fcb3a7baa7ba389fbb` |
| `supabase/repeatable/04102026_0260_banking_pay_next_simple_cancel_v1.sql` | `d10f3468e3fa6f39bc20b9e792f1d228259e5d234d433397cf5f552b9588135f` |
| `supabase/repeatable/04102026_0460_banking_pay_next_case_cancel_v1.sql` | `4417b25f96d6caefb627eae013c0c956268356104be15e2411dead56cc60bcf5` |
| `supabase/repeatable/04102026_2102_banking_pay_next_destination_cancel_v1.sql` | `2f9b25165d1cca79ace54ef8a5f794f69b46a1f2e8e3b5a58ca211ba1e62dd6a` |
| `supabase/repeatable/04102026_1827_banking_pay_next_candidate_destination_group_v1.sql` | `33891038c2865684c091f7bd3df4ce188bb42e026129163216e3bc341feb198c` |
| `supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql` | `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b` |
| `supabase/repeatable/04102026_0490_banking_pay_next_internal_zero_settlement_v1.sql` | `88a5679e4daeb440c36402f11c866394030af43509fe3520d71bb5399e5ef47c` |
| `broker/src/bpay-next-preparation.js` | `9271ec0086bffdd9779707bf44407eba0923b1392317eb142e46c8f8e0338749` |
| `docs/bpay-next/final-definer-closures.json` | `a0cf5d67e0e46568831bb54550680bcbac083af7b7263862dba9ac9af35d1bfd` |
