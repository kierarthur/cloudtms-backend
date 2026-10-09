# F-25 delayed case and Source consumer closure

Read-only finite design, 9 October 2026. No product/SQL execution, runtime, fixtures, tests or hosted calls. Root is the product writer. Authority is current 2153 accepted-input schema, 2225 provenance addition, 2247 acceptance capture/tail and 2236 PREPARE/PIN consumers, with saved 2254 transaction fence, 2256 CASE/WEEK insert guards and 2258 week-origin/eligibility successors. This is a finite edit map, not a proposed replacement architecture. SQL below is design material only; it was not installed or executed.

## 1. NO_CASE and SEALED_SELECTION consumers

The accepted Candidate row is uniquely `(prepare_command_id,candidate_id)` and `(run_id,candidate_id)`. Its `case_scope` is exactly `NO_CASE` or `SEALED_SELECTION`, with null or exact `case_selection_revision`. 2247:201–214 chooses this at the original acceptance; 2247:331–386 retains each exact selection item, rule/revision, case status, original funding witness and CASE_CREATE/ordinary WORK_COLLECTION origin. Do not repeat that discovery at FINANCE_ALLOCATE.

At each delayed owner, fetch this existing immutable authority through the actual job/run-command relation before interpreting scope:

```sql
select a.* into strict v_accepted
from private.bpay_next_accepted_input a
where a.run_id=v_run_id and a.prepare_command_id=v_job.command_id
  and a.candidate_id=v_candidate and a.module_epoch=v_job.module_epoch;
```

Retain current module/lease/run/worker locks, exact DONE/lost-reply returns, cursor checks, computation ownership and count/seal validation. The receipt is not an execution-epoch override.

| Final current owner | Bounded successor edits |
|---|---|
| `private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer)` — ordinary 1958:12, successor of 0360/raw0549 | At :107–111 dispatch `NO_CASE` using `v_accepted.case_scope`, not live EXISTS against finance_case or latest case_selection. NO_CASE has no accepted case children; unexpected children are damaged proof. SEALED_SELECTION resolves the header with the **accepted revision** :131, verifies SEALED and exact saved counts/manifest, and never selects today's latest revision. All initial/resumed Source comparison refusals still run before stage writes, now scoped to the same retained accepted_case children/origin and exact accepted revision. Keep `BPAY_NEXT_COMPARISON_NOT_INSTALLED`; F25 adds no Source comparison calculator. |
| Same 1958 initial state :149–204 | Use `v_accepted.target_pay_channel`, captured `candidate_floor/floor_state`, pay_date/week and valuation policy. Replace live Candidate floor :149 and run-date re-derivation :153. `floor_state='UNBOUND'` remains the existing case input refusal where floor is needed; do not invent zero. Keep the exact nonempty GROSS_DEDUCT/OVERPAYMENT count proof for NOT_AFFECTED. Build `expected_instruction_count` from selected accepted_case children and compare to the immutable accepted selection count. |
| Same 1958 WEEK_CAPTURE :216–249 | Enumerate `bpay_next_accepted_week_input` by accepted_input_id and existing `(original_pay_date,original_created_at_utc,original_run_worker_id)` cursor, not current worker_week_contribution. Use captured `eligible_arranged_amount`, eligibility/payment/return witness and both original economic/eligibility command sequences from 2225. Apply strict `<v_accepted.accepted_sequence` checks, matching 2256:39–42, original Candidate/week/run-worker identity and retained economic_origin_proof. The accepted receipt has already rejected missing/unresolved prior facts; do not substitute today's contribution or use a later return/projection. Preserve page size/cursor semantics. |
| Same 1958 INSTRUCTION_CAPTURE :253–450 | Enumerate selected accepted_case children by selection_no. Lock real finance_case/component/period for money; fetch `bpay_next_case_rule` using child's `rule_id,rule_revision` and exact case/Candidate FK. Do not require current_rule_id/current_rule_revision or component.rule_id still equal the retained old rule. Read status/due eligibility/funding eligibility and origin/policy from accepted evidence. Actual principal/funded/recovered/written-off/paid-credit/active-hold counters still determine conservation/capacity under the locked money owner. Retain same-channel PAYE qualification and actual original CASE_CREATE or ordinary WORK_COLLECTION proof. |
| Same 1958 ALLOCATE :470 onward | It already consumes frozen run_case_instruction, allocation state and scalar headroom. Change only its selection/count joins to exact accepted identity as needed; retain positive reservation, locked case/component/period counters, ACTIVE hold/capacity-use creation, exact scalar revisions and result immutability. A later status/rule pointer alone does not recapture terms. A real consumed money capacity cannot be restored from an old receipt. |
| `private.bpay_next_complete_no_case_worker_v1(uuid,uuid,bigint)` — 1630:8 | Replace live finance_case EXISTS :70–75 with exact `v_accepted.case_scope='NO_CASE'`, null accepted selection revision and no accepted case children. Keep no financial_resolution, captured position=line count, original genuine lease/owner and all holds. A later case belongs to the next Draft; it must not turn a correctly accepted NO_CASE worker into CASE_ALLOCATION_REQUIRED. Explicitly keep worker case_selection_revision=0. Preserve call to no-case week fact and READY/DONE transition. |
| `private.bpay_next_publish_no_case_week_fact_v1(uuid,uuid)` — 0420:7 | Resolve accepted_input through worker/run/PREPARE command. Require NO_CASE, captured PAYE target and actual existing READY owner. Replace live Candidate floor :31 and week :30 with captured floor_state/candidate_floor/pay_week_start. If UNBOUND and no existing week, preserve existing explicit absence/notice; this payment does not require a debt floor. Keep actual worker_week/contribution FOR UPDATE, real delta updates and PAYE_MANUAL projection's current genuine net-entry owner/idempotency. F25 does not freeze a future payroll-net entry before it exists. |

### Exact case term / live money split

The immutable accepted_case child already stores `rule_id,rule_revision,case_status,next_due_week_start,funded_eligible,original_funding_event_id,original_funded_at_utc,economic_origin_kind,case_create_command_id,work_collection_id,valuation_policy_id` plus accepted selection identity. Immutable rule supplies weekly due, schedule/floor/threshold and age semantics. The original selected component fact supplies case_kind/subtype/tax/instruction/payroll stage/source channel; keep exact original fact qualification. `case_revision/component_revision` saved at acceptance are provenance, not a reason to refuse because later valid money postings advanced those counters.

1958:343 also reads `v_case.due_date`. **2153 accepted_case has no due_date field, but current origin authority resolves it without another column.** 0330:9–11 forbids UPDATE/DELETE of `bpay_next_case_create_request`; its accept owner saves actual `due_date` :133–137 and apply owner inserts that exact request term into finance_case :223–225. Resolve CASE_CREATE `v_due_date` from the accepted child's `case_create_command_id` and original immutable request. The current ordinary WORK_COLLECTION generator in 2018:4193–4197 omits due_date, so its original due_date is null. Compare this origin-derived value to captured accepted pay_date; do not use today's finance_case.due_date or substitute next_due_week_start. Any future mutable due-date owner would need its own accepted term, but it is not present in these supported origins.

Full canonical `rg 'due_date' supabase` census found one NEXT finance_case due_date writer: the 0330 insert above. Core 1324:366 has nullable due_date with no default. Old/current WORK_COLLECTION inserts (1519:538 and final 2018:4193) omit it. Every existing case update owner changes rule pointer, hold/capacity, funded/paid/recovered/written-off principal or automatic ordinary correction/status (2018:4224–4237), **none sets due_date**. Current case rule/transfer owners can advance original funding schedule; they do not rewrite the request or case due date. Remaining hits are the 0360/1958 readers and history projection; the newly drafted but unapplied 2300 column/2303 capture are root's work, not an earlier mutable owner. This is a source-owner proof, not a claim that arbitrary owner SQL cannot alter a private row.

```sql
-- v_case_input is the selected immutable accepted_case_input child.
if v_case_input.economic_origin_kind='CASE_CREATE' then
  select r.due_date into strict v_due_date
  from private.bpay_next_case_create_request r
  where r.command_id=v_case_input.case_create_command_id
    and r.case_id=v_case_input.case_id and r.candidate_id=v_case_input.candidate_id;
elsif v_case_input.economic_origin_kind='WORK_COLLECTION' then
  -- First verify the exact accepted collection FK and its genuine original
  -- ordinary paid identity. That supported creator has no due-date term.
  v_due_date:=null;
else
  -- Existing CASE_INPUT_UNBOUND path; never fall back to current finance_case.
end if;
if v_case_input.case_status<>'OPEN'
  or (v_due_date is not null and v_due_date>v_accepted.pay_date) then
  v_nominal:=0;
end if;
```

1958 :354–401 creates/locks the real pay-week period and supports an original-funding-only rule advance. Keep the actual opening_due, realised recovery, active capacity and period identity. Never overwrite a nonzero consumed period to the old snapshot. Resolve period economic rule against retained accepted rule/valid original funding witness; a later schedule/policy update cannot migrate this Draft. A genuinely incompatible already-consumed period must retain its typed integrity/capacity refusal. NO_CASE floor capture does not rewrite a current worker-week's actual arranged totals.

2247 acceptance currently calls `bpay_next_stage_accepted_week_v1` only under SEALED_SELECTION :392. **This still needs the exact NOT_AFFECTED pre-clock exemption:** its unconditional call can reject unavailable prior weekly inputs which the selected OVERPAYMENT gross-only allocation never consumes. 0420 NO_CASE publication creates a new current contribution from this actual worker/projection; it does not need to replay all accepted prior contribution children. Do not invent a NO_CASE history sweep.

### Exact pre-clock NOT_AFFECTED gate

At the end of the existing staged CASE loop, after its selected-Source refusal and genuine selected CASE_CREATE/ordinary collection qualification, and before calling `stage_accepted_week_v1`, reproduce 1958:158–167 against the exact locked SEALED header/revision. Capture the following boolean in the stage owner's local variable; it is not a new money field:

```sql
select count(*)=v_case_selection.selected_count and count(*)>0
  and coalesce(bool_and((fc.case_kind,cc.case_kind,cc.instruction_kind,cc.payroll_stage)
    is not distinct from ('OVERPAYMENT','OVERPAYMENT','RECOVERY','GROSS_DEDUCT')),false)
into v_week_not_affected
from private.bpay_next_case_selection_item si
left join private.bpay_next_case_component cc
  on cc.id=si.case_component_id and cc.case_id=si.case_id and cc.candidate_id=si.candidate_id
left join private.bpay_next_finance_case fc
  on fc.id=si.case_id and fc.candidate_id=si.candidate_id
where si.run_id=v_case_selection.run_id and si.candidate_id=v_case_selection.candidate_id
  and si.selection_revision=v_case_selection.selection_revision and si.is_selected;
if not v_week_not_affected then
  perform private.bpay_next_stage_accepted_week_v1(p_run_id,p_command_id,v_input.id,v_input.candidate_id);
end if;
```

Keep the header's genuine SEALED/page/item/selected/excluded count proof and all accepted CASE children. Empty selection is **not** NOT_AFFECTED; missing identity/count mismatch is **not** proof of independence; never use inner joins that hide missing rows. Only selected items contribute to the gross-only predicate and expected instruction count. An excluded Source row remains in the sealed manifest/capture but neither needs a comparison calculator nor defeats an otherwise exact selected gross-only proof. Any selected Source collection throws `BPAY_NEXT_COMPARISON_NOT_INSTALLED` before the skip; it cannot exploit a gross-only tuple to bypass the comparison boundary. Selection closure counts still cover **all** original children; instruction closure count covers only the selected set. Do not replace either with the other's count.

The delayed 1958 successor re-proves the exact selected accepted set before starting INSTRUCTION_CAPTURE, then its existing capture :274–278 refuses any different tuple. Its final frozen instruction proof :470–490 requires nonempty count=expected_instruction_count, exact `(OVERPAYMENT,RECOVERY,GROSS_DEDUCT,GROSS_RECOVERY)` and every selected selection-item join, worker/captured counts and exact SEALED header count. Preserve all three proofs. For NOT_AFFECTED, no accepted WEEK children are needed or interpreted; ordinary debt-dependent selected sets still use genuine captured weekly origins. This exemption skips only the unused prior weekly operand; it does not relax the existing captured floor validity or current actual capacity/conservation guards.

### Existing ordinary origin trigger dependency

1519 `bpay_next_work_collection_capture_guard_v1()` :66–158 is invoked on run_position, run_case_instruction and case_period (:703/:706/:709). Its ordinary origin reader `bpay_next_work_collection_origin_v1(uuid,uuid,uuid)` deliberately avoids WORK.current_revision_id; it reads the real applied position/observed counters and genuine original ordinary paid liability. Keep its original identity, PAYE/TAXABLE/GROSS_DEDUCT qualification and actual principal/native consistency as money-owner constraints. Do not globally weaken this shared reader to tolerate arbitrary live books.

If the accepted successor no longer calls the current origin reader for an instruction's **terms**, the trigger must not silently reintroduce that terms discovery: bind the exact accepted_case_input via new instruction.run_id/Candidate/exact selection revision, selected case/component and work_collection_id; use accepted original valuation_policy_id, immutable original collection identity/paid effect/run-line/transfer and retained rule. Keep actual case/component/period principal/recovered/written-off/active-capacity equality. The original collection identity fields are guarded immutable by 1519:171–179. Actual observed money state is still checked; a later merely queued WORK.current_revision_id is not checked today and requires no exception.

The managed run_position branch :128–140 needs the equivalent exact cut/member match if its captured nominal/revision comes from accepted input: its current `(captured_revision_id,approved_source_ex_vat,realised_source_ex_vat)=(observed_revision_id,observed_approved,observed_realised)` must be split into accepted immutable line/revision/nominal proof and actual realised/held SOURCE/TARGET/reconcile ownership. Preserve its actual PREPARE lease/RESERVE→ALLOCATE completion guard :146–155. A case_period has no run/accepted_input key: do not guess a latest accepted child there. Keep its current real origin/counter invariant, or pass a concrete existing active allocation-state/run proof from the owning PREPARE seam before any further narrowly justified change. No anonymous flag/session switch or fabricated receipt is a valid alternative.

0310 `bpay_next_case_instruction_insert_guard_v1()` :159–184 checks PREPARING run/worker, preparation revision and exact SEALED selection; it introduces no live rule/due-date/Source terms lookup and stays unchanged. Its result/hold guards continue enforcing frozen amount and actual result identity. This dependency census prevents changing the caller while an insert trigger still requires a different capture contract; it does not claim the later FC-15 work-collection feature chain is built.

## 2. Source helper and caller closure

### Existing authority and exact lookup

The approved terms are already immutable: `bpay_next_accepted_work_input`, its `bpay_next_accepted_work_member` children, and the sealed `bpay_next_effective_work_cut`. Use `source_binding_version_id`, `source_binding_sha256` and `source_origin_certificate` from the member, not `source_component_binding.current_binding_version_id`. The original publication command/sequence, selected line count and effective revision are compound FKs on the cut. 2153's effective-cut owner :421–499 proves the exact publication APPLIED/DONE, POSITION_APPLY command COMPLETE, actual job DONE, strict predecessor sequence `<accepted_sequence`, module identity and exact ALL/SUBSET membership after all earlier Candidate commands drain. 2236 PIN :533–561 stores that effective revision in run_work and retains its accepted CLEAR query pin.

The following lookup is the bounded join for capture/helper context; use explicit local records/aliases in the eventual bodies. It does not take another live authorisation or query decision:

```sql
select cut.id, cut.prepare_job_id, cut.effective_revision_id,
       input.id as accepted_input_id, input.accepted_sequence,
       wi.id as accepted_work_input_id, wi.origin_proof,
       member.effective_approved_line_id, member.source_binding_version_id,
       member.source_binding_sha256, member.source_origin_certificate
from private.bpay_next_effective_work_cut cut
join private.bpay_next_accepted_input input on input.id=cut.accepted_input_id
join private.bpay_next_accepted_work_input wi on wi.id=cut.accepted_work_input_id
join private.bpay_next_accepted_work_member member on member.accepted_work_input_id=wi.id
join private.bpay_next_run_work rw
  on rw.work_id=cut.work_id and rw.captured_revision_id=cut.effective_revision_id
join private.bpay_next_run_worker worker on worker.id=rw.run_worker_id
where cut.prepare_job_id=p_job_id and cut.candidate_id=v_job.candidate_id
  and input.run_id=worker.run_id and input.prepare_command_id=v_job.command_id
  and worker.candidate_id=input.candidate_id and cut.work_id=v_work_id
  and member.component_key=v_component_key;
```

For settlement, derive the same cut from the frozen run_line→run_work→run_worker/run relation and that run's original PREPARE command. Its caller uses its **actual settlement** command/job lease, not an expired PREPARE lease. The retained cut remains evidence; it is not authority to execute under an old module epoch.

### Finite helper successors

Keep the existing strict current readers for publication, Workbench/current eligibility and current Daily callers. Add distinct private accepted readers; no `ignore_current` switch. No additional seed table or duplicate money counter is needed: 2153 migration :212–216 already adds `bpay_next_position.source_book_seed_effective_cut_id` with FK to effective_work_cut.

| Proposed private signature | Exact contract and source adaptation |
|---|---|
| `bpay_next_initial_tsfin_accepted_assert_v1(uuid,uuid,text) returns void` (effective_cut, binding_version, component_key) | Adapt 0018 `bpay_next_initial_tsfin_bound_assert_v1(uuid)` :187–226 to the exact retained member certificate. Preserve candidate/work/revision/publication/root/version/generation/row-signature/financial/inventory/entitlement/detail/policy/certificate/component digest checks and immutable approved-line identity/classification. Do not call `initial_tsfin_certificate_v1` or enumerate today's effective inventory. |
| `bpay_next_accepted_source_snapshot_v1(uuid,uuid,bigint)` (effective_cut, binding_version, expected_book_revision) | Return the **same 21-column typed contract** as 1246 `bpay_next_bound_source_snapshot_v1(uuid,bigint)` :585–599. Its fields are qualification_state, selection_state, candidate_id, canonical_work_id/component_key, binding_version_id, source_book_revision, approval_work_id/revision_id, approved_line_id, head_id, source_receipt_id, source_channel, tax_treatment, classification_line_id, valuation_policy_id, approved_source_ex_vat and four native realised/held buckets. Derive nominal amount/channel/policy from the exact immutable effective approved line/revision/binding. Require exact accepted member/binding digest and supported retained origin, plus the actual locked READY book and expected monetary revision. Replace only current binding/WORK/applied pointer and current root/HEAD gates :609–650 with retained cut evidence. |
| `bpay_next_source_book_seed_proof_accepted_v1(uuid,text,uuid,uuid)` (effective_cut, component_key, seed_effect, seed_hold) | Return the same seven fields as 1246 `source_book_seed_proof_v1(uuid,text,uuid,uuid,uuid)` :295–302. Replace its current/applied revision and live ZERO removal HEAD tests :315–327/:395–414 with retained exact accepted line/binding/publication proof. Read actual scalar SOURCE/TARGET counters under the position lock. Keep typed ORIGINAL_POSITION paid-effect/ACTIVE-hold proof and all genuine settlement/transfer/member/line/source/target ownership conditions :337–390. ZERO requires **all actual SOURCE and TARGET realised/held counters zero**, not merely no prior child. Its basis line is the accepted member's immutable effective approved line; do not find an arbitrary current binding line. |
| `bpay_next_source_disposition_seed_accepted_v1(uuid,text,uuid default null,uuid default null)` (effective_cut, component_key, seed_effect, seed_hold) | Return the same `(qualification_state text,source_book_revision bigint)` as 1246 seed owner :420. Follow existing module→Candidate control→WORK→position lock order. UNBOUND uses accepted proof and sets `source_book_seed_effective_cut_id=cut.id` once alongside first-seed metadata, native buckets and book revision 1. READY reads/verifies the existing real book and returns it; it must not reseed or demand the later cut equal the original seed's cut/revision/effect/hold. First-seed provenance remains immutable. |
| Existing `bpay_next_source_book_guard_v1() returns trigger` successor | At 1246 :521 choose accepted proof only when new seed_effective_cut_id is nonnull; strict old proof remains for genuine retained non-F25 initialization. Compare the complete typed result as now. Include seed_effective_cut_id in the **UNBOUND→READY first-seed** paired JSON exclusion arrays :535–541. Do not add it to READY :477–486 exclusions: that existing comparison must keep the first cut immutable. Do not change money-delta derivation, prohibit caller native/cache writes, or increment book revision on pointer-only publication. |

The snapshot row count above is 21; preserve its actual ordered signature, including four native fields, rather than relying on an informal count. Keep its existing `selection_state='UNBOUND'` output unless an existing enum/consumer needs another value; the new accepted context is proven by the explicit cut parameter, not an invented selection token.

The saved initial certificate shape is intentionally narrower than 0018's full certificate. 2153 member guard :394–405 expects `{candidate_id,work_id,revision_id,command_id,origin,basis,certificate_digest,component:{component_id,component_sha256}}`; it has no top-level source_channel or components array. Obtain channel from the immutable binding/revision. Recompute `weekly_source_sha256_jsonb_v1('BPAY_NEXT_INITIAL_TSFIN_CERTIFICATE_V1',basis)` (0018:104) and compare retained digest to initial_certificate_digest. Compare component ID/hash to immutable binding and the approved line's exact `source-component:<uuid>#<hash>` evidence_ref. Recompute the existing initial binding_digest array using saved certificate fields. This avoids a live TSFIN/contract re-read while preserving the original certificate proof.

For COMMON_HEAD_RECEIPT use saved `wi.origin_proof.bindings[].publication_members` and exact immutable receipt/member tuples, binding_digest and publication command/revision identities. `source_binding_members_v1(receipt)` :11 is a current-root/current-HEAD reader; do not call it here. Retain the exact receipt request_digest/decision-bundle identity and original committed/publication proof. A later withdrawn/superseded current root/HEAD cannot invalidate the earlier accepted original receipt. PROTECTED DIRECT_NEXT authority, when present, remains its exact saved canonical receipt; current simple same-channel self-bound WORK/TAXABLE restrictions remain. This closure does not add a comparison or cross-channel calculator.

### Exact delayed callers and live money exceptions

| Final owner in 2018 | Bounded edits |
|---|---|
| `bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer)` :739 | Replace ALL/SUBSET enumeration :823–859 with exact accepted members joined through cut/run_work as above, ordered by existing work/component cursor and bounded LIMIT. Do not omit a member because today's applied pointer differs; a missing actual position is a refusal. Lock WORK before position :863. Read nominal `approved_source_ex_vat` from the immutable accepted approved line, not current position.approved_source_ex_vat :877/:895–905. Remove current/applied revision and nominal amount equality as terms gates, retaining exact member/line identity and real source/target counters. |
| Same capture SOURCE branch :929–994 | Fetch binding_version by member's saved UUID and digest. Preserve actual self-canonical WORK, single publication command, PAYE/UMBRELLA same target channel and WORK/TAXABLE restrictions. If actual book is UNBOUND, retain bounded original qualified work_collection effect and one ACTIVE-hold point witness selection :956–970; ambiguous holds still refuse. Call accepted seed, then accepted snapshot using actual current book revision. Insert run_position with frozen accepted nominal/line/binding and **current actual money**; residual is accepted nominal minus current realised and held source. Query pin :1043–1058 remains exact retained CLEAR scope/digest. Never reinstall the old current revision/approval into position. |
| `bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer)` :1161 | Resolve exact cut/member for frozen run_position. At :1304–1326 remove only current `position.applied_revision_id` and nominal `approved_source_ex_vat` equality. Retain exact realised/held SOURCE, source_basis_channel and every realised/held TARGET scalar equality against capture. At :1329 replace current bound snapshot with accepted snapshot and preserve expected **monetary** book revision, all four native bucket equalities, source/target/execution shape and existing one selected quote target valuer :1359. Preserve immutable run_line/ACTIVE hold insertion and actual money posting once. |
| `bpay_next_post_simple_outcome_page_v1(uuid,uuid,bigint,bigint,integer)` :2298 | Actual result ownership remains transfer/member/frozen line/ACTIVE hold, exact SOURCE reserved and TARGET frozen values :2410–2434. For F25-origin lines use retained cut/member/binding when an UNBOUND book really needs adoption; replace current binding/applied-pointer discovery :2437–2454 with accepted proof plus the actual retained ACTIVE hold. A valid original already-admitted non-F25 line keeps its old typed reconciliation branch, without a fabricated accepted receipt. Keep financial effect insertion/idempotence :2456, once-only SOURCE H→R and TARGET H→R posting :2471–2487, hold REALISED transition, original timesheet and actual work_collection reconciliation. No new query/current eligibility/target lookup during settlement. |

Actual monetary guards are not pointers. A consumed hold, changed SOURCE/TARGET counters between capture and hold, monetary book revision change, negative/mixed unexplained state or incompatible real basis/channel retains its current typed refusal. Existing 1430 position writer :541–551 retains source_basis_channel whenever any SOURCE/TARGET money exists; money-free current nominal pointer updates are separate. This design does not overwrite the real basis to make a frozen accepted channel executable. Book READY must remain consistent with the original seed channel and qualified native/scalar amounts; the opposite native buckets remain zero for the currently supported single-channel book.

The SOURCE book does not become accepted money: acceptance captures terms/origin, capture takes the current actual monetary revision, hold consumes against that revision/counters, and outcome consumes an actual admitted hold. Neither a saved certificate nor a later current pointer can mint capacity, restore spent principal, reprice a frozen line or permit posting the same H→R twice.

## 3. Source apply order and finite scope

The master places F25 after F05 and before F11. The original 1246/0018/2018 owners remain earlier applied dependencies. After current 2153/2225 schema, 2153 accepted guards/effective-cut owner, 2233/2247 acceptance, 2236 accepted PREPARE/PIN and latest 2254/2256/2258 capture/fence/origin owners, append F25 successors in this dependency order:

1. Accepted initial certificate assertion; accepted seed proof; accepted seed owner; accepted snapshot; source_book_guard successor. PostgreSQL PL/pgSQL call lookup permits definition before invocation, but install all before any consuming caller is admitted.
2. Exact 2018 capture, then hold, then outcome successors (with original signatures/owner/ACLs). This is the master capture→hold→outcome authority chain, not a second settlement owner.
3. 1958 prepare_case_page successor, 0420 publish_no_case_week_fact successor and 1630 complete_no_case_worker successor. Install week-fact before its caller; preserve this final identity in the definer inventory. In the master chain the relevant owners are prepare_case_chain F25→later CM/WB/DR/FC cards, capture F25→WB36→SP14/SP20→DR38→later CM cards, hold F25→WB36→SP20→DR38→later WB/CM cards. No later card is claimed here.

Every new private SECURITY DEFINER must retain audited logical `owner to postgres`, fixed search_path and `REVOKE ALL ... FROM public,anon,authenticated,service_role`; provider adapters map only the audited logical owner to CURRENT_USER. No browser/service grant is added. No additional mutation RPC or task fixture is required by this design. Update final-definition inventory/selection metadata and source receipts to the actual successor bodies when root implements; this investigation supplies no installation verdict.

## 4. Bounded unresolved limits

No missing due_date storage remains for these supported CASE_CREATE/ordinary origins. The concrete remaining implementation is the accepted helper/caller chain above. Signed/mixed native seed, unsupported cross-channel Source execution, two-command Source execution and unresolved original capacity conservation retain existing refusal rather than a new calculator. Future genuine PAYE net entry remains the current projection owner. A non-F25 already-admitted result is reconciled only through its real frozen child/hold/result identity. No accepted receipt is retroactively fabricated, and no later current pointer is made money authority.

## 5. Reviewed source checkpoint

SHA256 of saved canonical source at this investigation checkpoint. Root's additional unapplied 2300/2303 drafts were read only for the redundant due-date capture check; they are not counted as installed authority. Concurrent edits require refreshed hashes/re-read before implementation. These are source receipts only; no SQL compilation/install/runtime verdict is supplied.

| File (under supabase) | SHA256 |
|---|---|
| migrations/09102026_2153_banking_pay_next_accepted_input_v1.sql | `0289b53ad745d5ff5c3a3130ce596e0b5a155e3645a84f1c57b154f8774a5182` |
| migrations/09102026_2225_banking_pay_next_accepted_input_provenance_v1.sql | `c7d79426059729a7f85c787120fdc371ef312f9b832ba6576a13d33139a086c3` |
| repeatable/09102026_2153_banking_pay_next_accepted_input_v1.sql | `4462d4e9a45909ba5e428b1916f74a7eeacf99b73add469b64eae8f185e28810` |
| repeatable/09102026_2236_banking_pay_next_accepted_prepare_consumers_v1.sql | `2881978637f38c72a6db81ed982f11a0e684bae6bac54a24bfbbb76979c3203c` |
| repeatable/09102026_2247_banking_pay_next_input_order_tail_v1.sql | `d8b27fc254df323871eb0734812f499ccdc9c327a41cfa81018066d1aee2d911` |
| repeatable/09102026_2254_banking_pay_next_acceptance_transaction_fence_v1.sql | `e26f1d705341d4942103ac55caad3f51a7ca84673de270ec2697d6e5eaf869ab` |
| repeatable/09102026_2256_banking_pay_next_accepted_case_week_guards_v1.sql | `5226a8c39e53f7900a1dfd6a466b8991b51cdff2c8095c52c300825d759dfd6f` |
| repeatable/09102026_2258_banking_pay_next_week_origin_capture_v1.sql | `125c6c135aa344009a4ac9e29782e93877d1d6affb8c8fd58765b576786e96e3` |
| repeatable/09102026_1958_banking_pay_next_ordinary_case_week_applicability_v1.sql | `c6f7d426fa60d3c6cf5db4d5c386aeb8111f974e455bd475a10dcfecf0e5851b` |
| repeatable/03102026_1630_banking_pay_next_no_case_completion_v1.sql | `3816b38a9b0776e033abbc55fa010cf62523294070c09a9c8508e36ac2d63d9c` |
| repeatable/04102026_0420_banking_pay_next_no_case_week_fact_v1.sql | `335f80467b7eb434034c3aa67295355aa8af04cc9c0834e313a67b056d10766b` |
| repeatable/06102026_1246_banking_pay_next_source_book_binding_v1.sql | `2e9a297ff362e41dbfbadc869b211b37176cf76d06edd3e78808a2e4c2a57340` |
| repeatable/07102026_0018_banking_pay_next_initial_tsfin_daily_evidence_v1.sql | `fa256bfa14b93321e1749c7cbee2505baca4f4da2eef5d11ea6457bdfae120e2` |
| repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql | `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b` |
| repeatable/04102026_0330_banking_pay_next_case_creation_selection_v1.sql | `9dc328ef5fb4e172622f72a97ce3cb84184740c5fe2575eb67f8bf1dac5fe903` |
| repeatable/03102026_1430_banking_pay_next_position_writer_v1.sql | `395ead5cf46a1399ef4859f056d89508b0862f9b0fda8dde92454107f17602fa` |
| repeatable/04102026_1519_banking_pay_next_work_collection_v1.sql | `5dda125e81ee4dac9e78bd87f1618b4c59635f0125536fb856cc060dc381e988` |
| repeatable/04102026_0310_banking_pay_next_case_guards_v1.sql | `7dd5085d3307a0194b3745487cc19a952ee3b8cb11d03f096d275d23632500fe` |
